// Copyright 2019 Cuttle.ai. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

//Package datasets has the api handler related to the datasets for the platform
package datasets

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/cuttle-ai/file-uploader-service/config"
	"github.com/cuttle-ai/file-uploader-service/models/db"
	"github.com/cuttle-ai/file-uploader-service/routes"
	"github.com/cuttle-ai/file-uploader-service/routes/response"
	"github.com/cuttle-ai/octopus/interpreter"
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

	//doing a sanity check
	err = d.UpdateSanityCheck(appCtx)
	if err != nil {
		//bad request
		appCtx.Log.Error("sanity check failed for dataset update", d.ID, err.Error())
		response.WriteError(w, response.Error{Err: "Invalid Params " + err.Error()}, http.StatusBadRequest)
		return
	}

	//getting the dataset info
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
	)
}
