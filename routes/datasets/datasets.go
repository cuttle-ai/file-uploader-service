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

	//getting the dataset info
	dataset, err := db.GetDataset(appCtx, id)
	if err != nil {
		//error while getting the info
		appCtx.Log.Error("error while getting the info for datatset with id", id, err.Error())
		response.WriteError(w, response.Error{Err: "Couldn't fetch the info"}, http.StatusInternalServerError)
		return
	}

	appCtx.Log.Info("Successfully fetched the dataser info of", id)
	response.Write(w, response.Message{Message: "Successfully fetched the info", Data: dataset})
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
	)
}
