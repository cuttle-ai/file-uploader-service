// Copyright 2019 Cuttle.ai. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

//Package file has the handler to process the requests related to files
package file

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"

	authConfig "github.com/cuttle-ai/auth-service/config"
	bModels "github.com/cuttle-ai/brain/models"
	"github.com/cuttle-ai/db-toolkit/datastores/services"
	"github.com/cuttle-ai/file-uploader-service/config"
	libfile "github.com/cuttle-ai/file-uploader-service/file"
	"github.com/cuttle-ai/file-uploader-service/models"
	"github.com/cuttle-ai/file-uploader-service/models/db"
	"github.com/cuttle-ai/file-uploader-service/routes"
	"github.com/cuttle-ai/file-uploader-service/routes/response"
	"github.com/cuttle-ai/go-sdk/services/datastores"
	"github.com/cuttle-ai/go-sdk/services/octopus"
	"github.com/cuttle-ai/octopus/interpreter"
	"github.com/google/uuid"
)

//StartValidating will start validating a given file
func StartValidating(a *config.AppContext, f libfile.File) error {
	/*
	 * First we will get validate the file
	 * Then we will update the status in database
	 * We will delete the existing errors
	 * We will update the new errors if any
	 */
	//validating the file
	a.Log.Info("Started validating the file", f.ID())
	errs, err := f.Validate()
	if err != nil {
		//error while validating the file
		a.Log.Error("error while validating the file for the file", f.ID(), err)
	}

	//update the status in database
	a.Log.Info("Started updating the status of the file", f.ID())
	nErr := f.UpdateStatus(a)
	if nErr != nil {
		//error while updating the validation error status
		a.Log.Error("error while updating the validation error status in db for the file", f.ID(), nErr)
	}
	if err != nil {
		return err
	}
	if nErr != nil {
		return nErr
	}

	//deleteing the existing errors
	a.Log.Info("Started deleting the existing validation error of the file", f.ID())
	fR := db.FileUpload{}
	fR.ID = f.ID()
	err = fR.DeleteErrors(a)
	if err != nil {
		//error while deleting the errors in the file record
		a.Log.Error("error while deleting the existing file upload errors for", fR.ID, err)
		return err
	}

	//if errors are found, we need to record it
	a.Log.Info("Have found", len(errs), "errors while validating", f.ID())
	if len(errs) == 0 {
		//no errors so no need to go further
		return nil
	}

	//creating the errors
	a.Log.Info("Started storing the validation errors of the file", f.ID())
	errM := []models.FileUploadError{}
	for _, v := range errs {
		errM = append(errM, models.FileUploadError{FileUploadID: fR.ID, Error: v.Error()})
	}
	err = db.CreateErrors(a, errM)
	if err != nil {
		//error while creating the error records
		a.Log.Error("error while creating the file upload errors for", fR.ID, err)
		return err
	}
	a.Log.Info("File validation exited sucessfully for", f.ID())
	return fmt.Errorf("%+v", errs)
}

//Validate will start the process of validating a file
func Validate(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	/*
	 * We will get the app context
	 * Then we will try to parse the request param id
	 * Then we will get the file upload record from the database
	 * Then we will get the file corresponding to it and start the process of validating it
	 */

	//getting the app context
	appCtx := ctx.Value(routes.AppContextKey).(*config.AppContext)
	appCtx.Log.Info("Got a request to validate a fileupload by", appCtx.Session.User.ID)

	//parse the request param id
	idStr := r.URL.Query().Get("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		//bad request
		appCtx.Log.Error("error while parsing the file upload id", err.Error(), idStr)
		response.WriteError(w, response.Error{Err: "Invalid Params " + idStr + " as id of the file upload"}, http.StatusBadRequest)
		return
	}

	//we will get the db record for the file
	f := &db.FileUpload{}
	f.ID = uint(id)
	err = f.Get(appCtx)
	if err != nil {
		//error while getting the info
		appCtx.Log.Error("error while getting the info for file uploaded with id", id, err.Error())
		response.WriteError(w, response.Error{Err: "Couldn't fetch the info"}, http.StatusInternalServerError)
		return
	}

	lF, err := libfile.GetFile(f.Type, *f)
	if err != nil {
		//error while getting the info
		appCtx.Log.Error("error while getting the underlying file processor id", id, err.Error())
		response.WriteError(w, response.Error{Err: "Couldn't fetch the info"}, http.StatusInternalServerError)
		return
	}

	//now we start processing it
	go StartValidating(appCtx, lF)

	appCtx.Log.Info("Successfully started validating the file", id)
	response.Write(w, response.Message{Message: "Successfully started validating"})
}

//UpdateUpload updates the uploaded file with a new upload
func UpdateUpload(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	/*
	 * We will get the app context
	 * We will try to parse the id of the file
	 * We will try to get the append flag
	 * We will get the file model from the database
	 * Then we will get the file payload
	 * Then we will save the file in the same location that of the existing file, thus by replacing the original file
	 * Then delete all the existing errors and update the existing file validation errors
	 * Then we will start start the uploading pipeline
	 */
	//getting the app context
	appCtx := ctx.Value(routes.AppContextKey).(*config.AppContext)
	appCtx.Log.Info("Got a request to re-upload the dataset", appCtx.Session.User.ID)

	//parse the request param id
	idStr := r.URL.Query().Get("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		//bad request
		appCtx.Log.Error("error while parsing the uploaded file id", err.Error(), idStr)
		response.WriteError(w, response.Error{Err: "Invalid Params " + idStr + " as id of the uploaded file"}, http.StatusBadRequest)
		return
	}

	//getting the append flag
	appendFlagStr := r.URL.Query().Get("append")
	appendFlag := false
	if appendFlagStr == "true" {
		appendFlag = true
	}

	//we will get the db record for the file
	f := &db.FileUpload{}
	f.ID = uint(id)
	err = f.Get(appCtx)
	if err != nil {
		//error while getting the info
		appCtx.Log.Error("error while getting the info for file uploaded with id", id, err.Error())
		response.WriteError(w, response.Error{Err: "Couldn't fetch the info of the file upload"}, http.StatusInternalServerError)
		return
	}

	//parsing the multipart form
	//maximum we can parse 1Gb file size
	r.ParseMultipartForm(10 << 30)

	//we are getting the file
	file, _, err := r.FormFile("file")
	if err != nil {
		appCtx.Log.Error("error retrieving the File", err)
		response.WriteError(w, response.Error{Err: "Error while reading the uploaded file"}, http.StatusBadRequest)
		return
	}
	defer file.Close()

	//move the file
	nF, err := os.Create(f.Location)
	if err != nil {
		appCtx.Log.Error("error while moving the file to the processing location", f.Location, err.Error())
		response.WriteError(w, response.Error{Err: "Error while moving the uploaded file to a server location"}, http.StatusInternalServerError)
		return
	}
	defer nF.Close()
	_, err = io.Copy(nF, file)
	if err != nil {
		appCtx.Log.Error("error while moving the file to the processing location", f.Location, err.Error())
		response.WriteError(w, response.Error{Err: "Error while moving the uploaded file to a server location"}, http.StatusInternalServerError)
		return
	}

	//delete the existing errors and update the status of upload as uploaded
	err = f.DeleteErrorsAndUpdateStatus(appCtx)
	if err != nil {
		//error while deleting the existing errors and updatingt the status
		appCtx.Log.Error("error deleting the file upload errors and updating the status for", f.ID, err.Error())
		response.WriteError(w, response.Error{Err: "Error while updating the upload status"}, http.StatusInternalServerError)
		return
	}

	//starts the datastore uploading pipeline
	go StartPipelineProcess(appCtx, f, appendFlag)

	appCtx.Log.Info("Sucessfully updated the file for", f.ID)
	f.Location = ""
	response.Write(w, response.Message{Message: "Successfully uploaded the file", Data: f})
}

//StartProcessingColumns will start processing the columns of a given file
func StartProcessingColumns(a *config.AppContext, f libfile.File) error {
	/*
	 * First we will get the dataset corresponding to the file
	 * Then we will get all the columns associated with the file
	 * Then we will start identifying the columns
	 * Then we will save/update the columns identified
	 */
	//getting the dataset corresponding to the the file
	a.Log.Info("started identifying the columns in the file processor id", f.ID())
	fDb := db.FileUpload{}
	fDb.ID = f.ID()
	dSet, err := fDb.GetDataset(a)
	if err != nil {
		//error while getting the dataset associated with the file upload
		a.Log.Error("error while getting the dataset of the fileupload while identifying the columns in the file of id", f.ID(), err)
		return err
	}

	//getting the columns associated with the dataset
	nodes, err := dSet.GetColumns(a)
	if err != nil {
		//error while getting the nodes associated with the dataset
		a.Log.Error("error while getting the columns of the dataset while identifying the columns in the file of dataset id", dSet.ID, err)
		return err
	}

	//getting all the columns
	a.Log.Info("getting all the columns in the file processor id", f.ID())
	columns := []interpreter.ColumnNode{}
	columnsMap := map[string]bModels.Node{}
	for _, v := range nodes {
		columns = append(columns, v.ColumnNode())
		columnsMap[v.UID.String()] = v
	}

	//start identifying the columns
	columns, err = f.IdentifyColumns(columns)
	if err != nil {
		//error while identifying the columns in the dataset
		a.Log.Error("error while identifying the columns in the dataset id", dSet.ID, err)
		return err
	}
	a.Log.Info("identified the columns in the file of processor id", f.ID())

	//save/update the columns
	nodes = []bModels.Node{}
	for _, v := range columns {
		node, _ := columnsMap[v.UID]
		node.DatasetID = dSet.ID
		nodes = append(nodes, node.FromColumn(v))
	}
	nodes, err = dSet.UpdateColumns(a, nodes)
	if err != nil {
		//error while updating the columns in the database
		a.Log.Error("error while updating the columns in the database id", dSet.ID, err)
		return err
	}
	a.Log.Info("saved/updated the columns of the file of processor id", f.ID(), "found", len(nodes), "columns")
	return nil
}

//ProcessColumns will process the columns in an uploaded data file.
//If the columns are available, it will validate their data type same with file.
//Else it will identify the column and store them the database
func ProcessColumns(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	/*
	 * We will get the app context
	 * Then we will try to parse the request param id
	 * Then we will get the file upload record from the database
	 * Then we will get the file corresponding to it
	 * start the process for identifying the columns in the file
	 */

	//getting the app context
	appCtx := ctx.Value(routes.AppContextKey).(*config.AppContext)
	appCtx.Log.Info("Got a request to process the columns in fileupload by", appCtx.Session.User.ID)

	//parse the request param id
	idStr := r.URL.Query().Get("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		//bad request
		appCtx.Log.Error("error while parsing the file upload id", err.Error(), idStr)
		response.WriteError(w, response.Error{Err: "Invalid Params " + idStr + " as id of the file upload"}, http.StatusBadRequest)
		return
	}

	//we will get the db record for the file
	f := &db.FileUpload{}
	f.ID = uint(id)
	err = f.Get(appCtx)
	if err != nil {
		//error while getting the info
		appCtx.Log.Error("error while getting the info for file uploaded with id", id, err.Error())
		response.WriteError(w, response.Error{Err: "Couldn't fetch the info"}, http.StatusInternalServerError)
		return
	}

	lF, err := libfile.GetFile(f.Type, *f)
	if err != nil {
		//error while getting the info
		appCtx.Log.Error("error while getting the underlying file processor id", id, err.Error())
		response.WriteError(w, response.Error{Err: "Couldn't fetch the info"}, http.StatusInternalServerError)
		return
	}

	//now we start processing it
	go StartProcessingColumns(appCtx, lF)

	appCtx.Log.Info("Successfully started identifying the columns in the file", id)
	response.Write(w, response.Message{Message: "Successfully started identifying the columns"})
}

//StartUploadingToDatastore will start uploading the file to data store
func StartUploadingToDatastore(a *config.AppContext, f libfile.File, appendFlag bool) error {
	/*
	 * First we will get the dataset corresponding to the file
	 * Then we will try to get the table associated with the dataset
	 * Then we will get the list of datastore services
	 * Will choose the service with least number of datasets in it
	 * Create the table if necessary
	 * Then we will get all the columns associated with the file
	 * Then we will check whether the list of columns is not zero
	 * If table is created, we will update the PUID of the columns in database
	 * Then we start uploading the table to the datastore
	 * If the table is not created, then we will then update the table created flag as true
	 */
	//getting the dataset corresponding to the the file
	a.Log.Info("started uploading the table to the datastore with file processor id", f.ID())
	fDb := db.FileUpload{}
	fDb.ID = f.ID()
	dSet, err := fDb.GetDataset(a)
	if err != nil {
		//error while getting the dataset associated with the file upload
		a.Log.Error("error while getting the dataset of the fileupload while uploading the it to datastore in the file of id", f.ID(), err)
		return err
	}

	//getting the columns associated with the dataset
	a.Log.Info("getting table of the file processor id", f.ID())
	table, err := dSet.GetTable(a)
	if err != nil {
		//error while getting the table associated with the dataset
		a.Log.Error("error while getting the table associated with the dataset while uploading the it to datastore in the file of dataset id", dSet.ID, err)
		return err
	}

	//we will get the list of datastore services
	dS, err := datastores.ListDatastores(a.Log, config.DiscoveryURL, config.DiscoveryToken, authConfig.MasterAppDetails.AccessToken)
	if err != nil {
		//error while getting the list of datastores in the platform
		a.Log.Error("error while getting the list of datastores for uploading the datastore", dSet.ID, err)
		return err
	}
	if len(dS) == 0 {
		//couldn't find any data stores
		a.Log.Error("couldn't find any data stores for uploading the datastore", dSet.ID, err)
		return err
	}

	//choosing the dataset with least no. of datasets
	ser := services.Service{Datasets: 100000}
	for _, v := range dS {
		if ser.Datasets >= v.Datasets {
			ser = v
		}
	}

	//creating the table if necessary
	tableCreated := false
	if table.ID == 0 {
		a.Log.Info("creating the table for the file processor id", f.ID())
		//need to create the table
		tableNode := interpreter.TableNode{
			UID: uuid.New().String(),
		}
		tableNode.Name = "table_" + tableNode.UID
		tableNode.DatastoreID = ser.ID
		table.DatasetID = dSet.ID
		table = table.FromTable(tableNode)
		table, err = dSet.CreateTable(a, table)
		if err != nil {
			//error while saving the table to database
			a.Log.Error("error while saving the table associated with the dataset while uploading the it to datastore in the file of dataset id", dSet.ID, err)
			return err
		}
		tableCreated = true
	}

	//getting the columns associated with the dataset
	a.Log.Info("getting all the columns in the file processor id", f.ID())
	nodes, err := dSet.GetColumns(a)
	if err != nil {
		//error while getting the nodes associated with the dataset
		a.Log.Error("error while getting the columns of the dataset wwhile uploading the it to datastore in the file of dataset id", dSet.ID, err)
		return err
	}

	//if the list of columns is zero
	if len(nodes) == 0 {
		a.Log.Warn("No columns are found. So skipping uploading")
		return err
	}

	//if the table is created, update the puid of the columns and proceed ahead
	if tableCreated {
		a.Log.Info("updating the column's PUID with created table's puid the file processor id", f.ID())
		for i := 0; i < len(nodes); i++ {
			nodes[i].PUID = table.UID
		}
		//now update in database
		nodes, err = dSet.UpdateColumns(a, nodes)
		if err != nil {
			//error while updating the columns in the database
			a.Log.Error("error while updating the columns in the database id", dSet.ID, err)
			return err
		}
	}

	//getting all the columns
	columns := []interpreter.ColumnNode{}
	columnsMap := map[string]bModels.Node{}
	for _, v := range nodes {
		columns = append(columns, v.ColumnNode())
		columnsMap[v.UID.String()] = v
	}
	tableNode := table.TableNode()
	tableNode.Children = columns

	//we start uploading the table to the datastore
	a.Log.Info("going to upload the dataset to datastore for dataset id", dSet.ID, ser.Password, "with append as", appendFlag)
	err = f.Upload(a, tableNode, appendFlag, !dSet.TableCreated, ser)
	if err != nil {
		//error while uploading the table to datastore
		a.Log.Error("error while uploading the table to datastore", err)
		return err
	}

	a.Log.Info("successfull uploaded the dataset to a datastore for the dataset id", dSet.ID)
	if dSet.TableCreated {
		return err
	}

	dSet.TableCreated = true
	dSet.DatastoreID = ser.ID
	a.Log.Info("updating the table created flag to the dataset to a datastore for the dataset id", dSet.ID)
	err = dSet.Update(a)
	if err != nil {
		//error while updating the table created flag as true for the datastore
		a.Log.Error("error while updating the table created flag as true for the datastore", err)
		return err
	}
	return nil
}

//UploadToDatastore will upload the file to the datastore with minimum datasets stored in it
func UploadToDatastore(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	/*
	 * We will get the app context
	 * Then we will try to parse the request param id
	 * Then we will try to parse the request param to append/replace data
	 * Then we will get the file upload record from the database
	 * start the process for uploading it to a datastore
	 */

	//getting the app context
	appCtx := ctx.Value(routes.AppContextKey).(*config.AppContext)
	appCtx.Log.Info("Got a request to upload file to datstore by", appCtx.Session.User.ID)

	//parse the request param id
	idStr := r.URL.Query().Get("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		//bad request
		appCtx.Log.Error("error while parsing the file upload id", err.Error(), idStr)
		response.WriteError(w, response.Error{Err: "Invalid Params " + idStr + " as id of the file upload"}, http.StatusBadRequest)
		return
	}

	//parse the request param replace param
	appendData := r.URL.Query().Get("appendData")
	appendFlag := false
	if appendData == "true" {
		appendFlag = true
	}

	//we will get the db record for the file
	f := &db.FileUpload{}
	f.ID = uint(id)
	err = f.Get(appCtx)
	if err != nil {
		//error while getting the info
		appCtx.Log.Error("error while getting the info for file uploaded with id", id, err.Error())
		response.WriteError(w, response.Error{Err: "Couldn't fetch the info"}, http.StatusInternalServerError)
		return
	}

	lF, err := libfile.GetFile(f.Type, *f)
	if err != nil {
		//error while getting the info
		appCtx.Log.Error("error while getting the underlying file processor id", id, err.Error())
		response.WriteError(w, response.Error{Err: "Couldn't fetch the info"}, http.StatusInternalServerError)
		return
	}

	//now we start uploading it
	go StartUploadingToDatastore(appCtx, lF, appendFlag)

	appCtx.Log.Info("Successfully started uploading the file to the datastore in", id)
	response.Write(w, response.Message{Message: "Successfully started uploading the file to the datastore"})
}

//StartPipelineProcess will start the pipeline of uploading file to data store pipeline
func StartPipelineProcess(a *config.AppContext, fU *db.FileUpload, appendFlag bool) {
	/*
	 * We will get the file upload details
	 * We will get the file
	 * Then we will validate
	 * Then we will start processing the columns
	 * Then we will start uploading to data store
	 * Then we will update the dict
	 */
	//getting the file details
	err := fU.Get(a)
	if err != nil {
		//error while getting the info
		a.Log.Error("error while getting the info for file uploaded with id", fU.ID, err.Error())
		return
	}

	//getting the file
	f, err := libfile.GetFile(fU.Type, *fU)
	if err != nil {
		//error while getting the info
		a.Log.Error("error while getting the underlying file processor id", fU.ID, err.Error())
	}

	//start validating it
	err = StartValidating(a, f)
	if err != nil {
		//error while validating the file
		a.Log.Error("error while validating the uploaded file", err)
		return
	}

	//if append flag is not there, it means that we have identify the columns
	if !appendFlag {
		err = StartProcessingColumns(a, f)
		if err != nil {
			//error while processing the file
			a.Log.Error("error while processing the uploaded file", err)
			return
		}
	}

	//start uploading the data to the data store
	err = StartUploadingToDatastore(a, f, appendFlag)
	if err != nil {
		//error while uploading the file to data store
		a.Log.Error("error while uploading the file to data store", err)
		return
	}

	//update the user dict from octopus service memory
	err = octopus.UpdateDict(a.Log, config.DiscoveryURL, config.DiscoveryToken, a.Session.ID)
	if err != nil {
		//error while updating the dict from octopus
		a.Log.Error("error while updating the dict from the octopus service for user", a.Session.User.ID, err)
		return
	}
}

func init() {
	routes.AddRoutes(
		routes.Route{
			Version:     "v1",
			Pattern:     "/file/validate",
			HandlerFunc: Validate,
		},
		routes.Route{
			Version:     "v1",
			Pattern:     "/file/upload",
			HandlerFunc: UpdateUpload,
		},
		routes.Route{
			Version:     "v1",
			Pattern:     "/file/columns/process",
			HandlerFunc: ProcessColumns,
		},
		routes.Route{
			Version:     "v1",
			Pattern:     "/file/uploadtodatastore",
			HandlerFunc: UploadToDatastore,
		},
	)
}
