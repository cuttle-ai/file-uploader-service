// Copyright 2019 Cuttle.ai. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

//Package datasets has the api handler related to the datasets for the platform
package datasets

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cuttle-ai/brain/models"
	"github.com/cuttle-ai/file-uploader-service/config"
	"github.com/cuttle-ai/file-uploader-service/models/db"
	"github.com/cuttle-ai/file-uploader-service/routes"
	"github.com/cuttle-ai/file-uploader-service/routes/response"
	"github.com/cuttle-ai/go-sdk/services/datastores"
	"github.com/cuttle-ai/octopus/interpreter"

	authConfig "github.com/cuttle-ai/auth-service/config"
	fModels "github.com/cuttle-ai/file-uploader-service/models"
)

//GetDatasets will return the list of datasets for a given user
func GetDatasets(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	/*
	 * We will get the app context
	 * Then we will get the datasets for the current user session
	 */

	//getting the app context
	appCtx := ctx.Value(routes.AppContextKey).(*config.AppContext)
	appCtx.Log.Info("Got a request to get the datasets list by", appCtx.Session.User.ID)

	datasets, err := db.GetDatasets(appCtx)
	if err != nil {
		//error while getting the list
		appCtx.Log.Error("error while getting the list", err.Error())
		response.WriteError(w, response.Error{Err: "Couldn't fetch the list"}, http.StatusInternalServerError)
		return
	}

	appCtx.Log.Info("Successfully fetched the list of datasets of length", len(datasets))
	response.Write(w, response.Message{Message: "Successfully fetched the list", Data: datasets})
}

//GetDataSet will return the info about dataset for a given user
func GetDataSet(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	/*
	 * We will get the app context
	 * Then we will try to parse the request param id
	 * Then we will get the dataset info for the current user session
	 */

	//getting the app context
	appCtx := ctx.Value(routes.AppContextKey).(*config.AppContext)
	appCtx.Log.Info("Got a request to get the dataset info by", appCtx.Session.User.ID)

	//parse the request param id
	idStr := r.URL.Query().Get("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		//bad request
		appCtx.Log.Error("error while parsing the dataset id", err.Error(), idStr)
		response.WriteError(w, response.Error{Err: "Invalid Params " + idStr + " as id of the dataset"}, http.StatusBadRequest)
		return
	}
	d := &db.Dataset{}
	d.ID = uint(id)

	//getting the dataset info
	if appCtx.Session.User.UserType != authConfig.AdminUser && appCtx.Session.User.UserType != authConfig.SuperAdmin {
		d.UserID = appCtx.Session.User.ID
	} else if d.UserID == 0 {
		d.UserID = appCtx.Session.User.ID
	}
	err = d.Get(appCtx, true)
	if err != nil {
		//error while getting the info
		appCtx.Log.Error("error while getting the info for datatset with id", id, err.Error())
		response.WriteError(w, response.Error{Err: "Couldn't fetch the info"}, http.StatusInternalServerError)
		return
	}

	cols, err := d.GetColumns(appCtx)
	if err != nil {
		//error while getting the columns from the app dataset
		appCtx.Log.Error("error while getting the columns of datatset with id", id, err.Error())
		response.WriteError(w, response.Error{Err: "Couldn't fetch the info"}, http.StatusInternalServerError)
		return
	}
	iCols := []interpreter.ColumnNode{}
	for _, v := range cols {
		iCols = append(iCols, v.ColumnNode())
	}
	appCtx.Log.Info("Successfully fetched the dataset info of", id)
	response.Write(w, response.Message{Message: "Successfully fetched the info", Data: struct {
		Dataset *db.Dataset
		Columns []interpreter.ColumnNode
	}{d, iCols}})
}

//UpdateDataset will update a dataset for a given user
func UpdateDataset(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	/*
	 * We will get the app context
	 * Then we will try to parse the request param dataset
	 * We will do a sanity check
	 * Then we will update the dataset in db
	 */

	//getting the app context
	appCtx := ctx.Value(routes.AppContextKey).(*config.AppContext)
	appCtx.Log.Info("Got a request to update the dataset info by", appCtx.Session.User.ID)

	//parse the request param dataset
	d := &db.Dataset{}
	err := json.NewDecoder(r.Body).Decode(d)
	if err != nil {
		//bad request
		appCtx.Log.Error("error while parsing the dataset", err.Error())
		response.WriteError(w, response.Error{Err: "Invalid Params " + err.Error()}, http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	//doing a sanity check
	err = d.UpdateSanityCheck(appCtx)
	if err != nil {
		//bad request
		appCtx.Log.Error("sanity check failed for dataset update", d.ID, err.Error())
		response.WriteError(w, response.Error{Err: "Invalid Params " + err.Error()}, http.StatusBadRequest)
		return
	}

	//getting the dataset info
	if appCtx.Session.User.UserType != authConfig.AdminUser && appCtx.Session.User.UserType != authConfig.SuperAdmin {
		d.UserID = appCtx.Session.User.ID
	}
	err = d.Update(appCtx)
	if err != nil {
		//error while getting the info
		appCtx.Log.Error("error while updating datatset with id", d.ID, err.Error())
		response.WriteError(w, response.Error{Err: "Couldn't update the info"}, http.StatusInternalServerError)
		return
	}

	appCtx.Log.Info("Successfully updated the dataset info of", d.ID)
	response.Write(w, response.Message{Message: "Successfully updated the info", Data: d})
}

func startDeletingDataset(a *config.AppContext, d *db.Dataset) {
	/*
	 * If the dataset type is file,
	 * 		we will remove the physical files associated with the dataset
	 * Then we will remove the data from datastore
	 * We will get the table name
	 * We will get the info of datastores from data stores services
	 * Then we will remove the dataset and all the cascaded information from database
	 */
	//if the dataset source is of the type file do the following
	if d.Source == models.DatasetSourceFile {
		//remove the physical files
		f, ok := d.UploadedDataset.(fModels.FileDataset)
		if !ok {
			//couldn't type cast the file data source as fileDataset
			a.Log.Error("error while inferring the dataset type to fileDataset with the datasource as file")
			return
		}

		//removing the physical files
		sepa := string([]rune{filepath.Separator})
		splits := strings.Split(f.Info.Location, sepa)
		direct := strings.Join(splits[:len(splits)-1], sepa)
		a.Log.Info("removing the physical files from", direct)
		err := os.RemoveAll(direct)
		if err != nil {
			//error while removing the dataset's physical files
			a.Log.Error("error while removing the physical files for the dataset", d.ID, f.Info.Location, err)
			return
		}
	}

	//remove the data from datastore
	if d.TableCreated {
		err := deleteDatasetFromDatastore(a, d)
		if err != nil {
			//error while removing the dataset from the datastore
			a.Log.Error("error while removing the dataset from the datastore", d.ID, err)
			return
		}
	}

	//remove the dataset and all the cascaded information from database
	err := d.Delete(a)
	if err != nil {
		//error while removing the db info from the database
		a.Log.Error("error while removing the db info from the database", d.DatastoreID, err)
		return
	}
}

func deleteDatasetFromDatastore(a *config.AppContext, d *db.Dataset) error {
	/*
	 * We will get the dataset's table
	 * then we will get the info about the datastoring the dataset info
	 * then we will get the datastore
	 * then we will delete the table in the datastore
	 */
	//getting the dataset's table
	a.Log.Info("getting table of the dataset of id", d.ID)
	table, err := d.GetTable(a)
	if err != nil {
		//error while getting the table associated with the dataset
		a.Log.Error("error while getting the table associated with the dataset while removing it from datastore of dataste id", d.ID, err)
		return err
	}

	//getting the info about the service
	dS, err := datastores.GetDatastore(a.Log, config.DiscoveryURL, config.DiscoveryToken, a.Session.ID, d.DatastoreID)
	if err != nil {
		//error while getting the info of datastores in the platform
		a.Log.Error("error while getting the info of datastores for removing the datastore", d.DatastoreID, err)
		return err
	}
	//no datastore found
	if dS == nil {
		a.Log.Info("Couldn't find any datastore where the data is stored for dataset", d.ID, "with datastore id", d.DatastoreID)
		return nil
	}

	//getting the datastore
	dst, err := dS.Datastore()
	if err != nil {
		//error while the datastore from the service
		a.Log.Error("error while getting the datastore connection from the datastore", d.DatastoreID, err)
		return err
	}
	if dst == nil {
		a.Log.Info("Couldn't find any datastore with id", dS.ID, "for deleting the datastore", d.ID)
		return nil
	}

	//deleting the table
	err = dst.DeleteTable("table_" + table.UID.String())
	if err != nil {
		//error while deleting the table from datastore
		a.Log.Error("error while deleting the table from datastore", d.DatastoreID, "table_"+table.UID.String(), err)
		return err
	}
	return nil
}

//DeleteDataset will delete a given dataset for a given user and all the information related to that
func DeleteDataset(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	/*
	 * We will get the app context
	 * Then we will try to parse the request param dataset
	 * We will get the information about the dataset
	 * We will do a sanity check
	 * Then we will start deleting the dataset from the platform
	 */

	//getting the app context
	appCtx := ctx.Value(routes.AppContextKey).(*config.AppContext)
	appCtx.Log.Info("Got a request to delete the dataset from platform by", appCtx.Session.User.ID)

	//parse the request param dataset
	d := &db.Dataset{}
	err := json.NewDecoder(r.Body).Decode(d)
	if err != nil {
		//bad request
		appCtx.Log.Error("error while parsing the dataset", err.Error())
		response.WriteError(w, response.Error{Err: "Invalid Params " + err.Error()}, http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	//getting the information about the dataset
	if appCtx.Session.User.UserType != authConfig.AdminUser && appCtx.Session.User.UserType != authConfig.SuperAdmin {
		d.UserID = appCtx.Session.User.ID
	}
	err = d.Get(appCtx, false)
	if err != nil {
		//error while getting the info
		appCtx.Log.Error("error while getting the info for datatset with id", d.ID, err.Error())
		response.WriteError(w, response.Error{Err: "Couldn't fetch the info"}, http.StatusInternalServerError)
		return
	}

	//doing a sanity check
	err = d.DeleteSanityCheck(appCtx)
	if err != nil {
		//bad request
		appCtx.Log.Error("sanity check failed for dataset delete", d.ID, err.Error())
		response.WriteError(w, response.Error{Err: "Invalid Params " + err.Error()}, http.StatusForbidden)
		return
	}

	//start deleting the dataset from the platform
	go startDeletingDataset(appCtx, d)

	appCtx.Log.Info("Successfully started deleting the dataset", d.ID)
	response.Write(w, response.Message{Message: "Successfully initiated deleting of the dataset", Data: nil})
}

func init() {
	routes.AddRoutes(
		routes.Route{
			Version:     "v1",
			Pattern:     "/datasets/list",
			HandlerFunc: GetDatasets,
		},
		routes.Route{
			Version:     "v1",
			Pattern:     "/datasets/get",
			HandlerFunc: GetDataSet,
		},
		routes.Route{
			Version:     "v1",
			Pattern:     "/dataset/update",
			HandlerFunc: UpdateDataset,
		},
		routes.Route{
			Version:     "v1",
			Pattern:     "/dataset/delete",
			HandlerFunc: DeleteDataset,
		},
	)
}
