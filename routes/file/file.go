// Copyright 2019 Cuttle.ai. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

//Package file has the handler to process the requests related to files
package file

import (
	"context"
	"net/http"
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

func init() {
	routes.AddRoutes(
		routes.Route{
			Version:     "v1",
			Pattern:     "/file/validate",
			HandlerFunc: Validate,
		},
	)
}
