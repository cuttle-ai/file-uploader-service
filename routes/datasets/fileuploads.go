// Copyright 2019 Cuttle.ai. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

//Package datasets has the api handler related to the datasets for the platform
package datasets

import (
	"context"
	"net/http"
	"strconv"

	"github.com/cuttle-ai/file-uploader-service/config"
	"github.com/cuttle-ai/file-uploader-service/models/db"
	"github.com/cuttle-ai/file-uploader-service/routes"
	"github.com/cuttle-ai/file-uploader-service/routes/response"
)

//GetFileUploads will returns the list of file uploads for a given user
func GetFileUploads(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	/*
	 * We will get the app context
	 * Then we will get the file uploads for the current user session
	 */

	//getting the app context
	appCtx := ctx.Value(routes.AppContextKey).(*config.AppContext)
	appCtx.Log.Info("Got a request to get the file upload list by", appCtx.Session.User.ID)

	uploads, err := db.GetFileUploads(appCtx)
	if err != nil {
		//error while getting the list
		appCtx.Log.Error("error while getting the list", err.Error())
		response.WriteError(w, response.Error{Err: "Couldn't fetch the list"}, http.StatusInternalServerError)
		return
	}

	appCtx.Log.Info("Successfully fetched the list of file uploads of length", len(uploads))
	response.Write(w, response.Message{Message: "Successfully fetched the list", Data: uploads})
}

//GetFileUpload will returns the info about file upload for a given user
func GetFileUpload(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	/*
	 * We will get the app context
	 * Then we will try to parse the request param id
	 * Then we will get the file upload info for the current user session
	 */

	//getting the app context
	appCtx := ctx.Value(routes.AppContextKey).(*config.AppContext)
	appCtx.Log.Info("Got a request to get the file upload info by", appCtx.Session.User.ID)

	//parse the request param id
	idStr := r.URL.Query().Get("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		//bad request
		appCtx.Log.Error("error while parsing the upload id", err.Error(), idStr)
		response.WriteError(w, response.Error{Err: "Invalid Params " + idStr + " as id of the fileupload"}, http.StatusBadRequest)
		return
	}

	//getting the upload info
	upload, err := db.GetFileUpload(appCtx, id)
	if err != nil {
		//error while getting the info
		appCtx.Log.Error("error while getting the info", err.Error())
		response.WriteError(w, response.Error{Err: "Couldn't fetch the info"}, http.StatusInternalServerError)
		return
	}

	appCtx.Log.Info("Successfully fetched the upload info of", id)
	response.Write(w, response.Message{Message: "Successfully fetched the info", Data: upload})
}

func init() {
	routes.AddRoutes(
		routes.Route{
			Version:     "v1",
			Pattern:     "/datasets/list",
			HandlerFunc: GetFileUploads,
		},
		routes.Route{
			Version:     "v1",
			Pattern:     "/datasets/get",
			HandlerFunc: GetFileUpload,
		},
	)
}
