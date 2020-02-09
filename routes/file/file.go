// Copyright 2019 Cuttle.ai. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

//Package file has the handler to process the requests related to files
package file

import (
	"context"
	"io"
	"net/http"
	"os"
	"strconv"

	"github.com/cuttle-ai/file-uploader-service/config"
	libfile "github.com/cuttle-ai/file-uploader-service/file"
	"github.com/cuttle-ai/file-uploader-service/models"
	"github.com/cuttle-ai/file-uploader-service/models/db"
	"github.com/cuttle-ai/file-uploader-service/routes"
	"github.com/cuttle-ai/file-uploader-service/routes/response"
)

func startValidating(a *config.AppContext, f libfile.File) {
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
	if err != nil || nErr != nil {
		return
	}

	//deleteing the existing errors
	a.Log.Info("Started deleting the existing validation error of the file", f.ID())
	fR := db.FileUpload{}
	fR.ID = f.ID()
	err = fR.DeleteErrors(a)
	if err != nil {
		//error while deleting the errors in the file record
		a.Log.Error("error while deleting the existing file upload errors for", fR.ID, err)
		return
	}

	//if errors are found, we need to record it
	a.Log.Info("Have found", len(errs), "errors while validating", f.ID())
	if len(errs) == 0 {
		//no errors so no need to go further
		return
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
	}
	a.Log.Info("File validation exited sucessfully for", f.ID())
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
	go startValidating(appCtx, lF)

	appCtx.Log.Info("Successfully started validating the file", id)
	response.Write(w, response.Message{Message: "Successfully started validating"})
}

//UpdateUpload updates the uploaded file with a new upload
func UpdateUpload(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	/*
	 * We will get the app context
	 * We will try to parse the id of the file
	 * We will get the file model from the database
	 * Then we will get the file payload
	 * Then we will save the file in the same location that of the existing file, thus by replacing the original file
	 * Then delete all the existing errors and update the existing file validation errors
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

	appCtx.Log.Info("Sucessfully updated the file for", f.ID)
	f.Location = ""
	response.Write(w, response.Message{Message: "Successfully uploaded the file", Data: f})
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
	)
}
