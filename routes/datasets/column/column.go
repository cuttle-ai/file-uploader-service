// Copyright 2019 Cuttle.ai. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

//Package column has the api handler for creating/updating/deleting columns in a dataset
package column

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/cuttle-ai/brain/models"
	"github.com/cuttle-ai/file-uploader-service/config"
	"github.com/cuttle-ai/file-uploader-service/routes"
	"github.com/cuttle-ai/file-uploader-service/routes/response"
)

//UpdateColumn updates a given column in database and inform the octopus service to update the dict
func UpdateColumn(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	/*
	 * We will get the app context
	 * Then we will parse the node metadata
	 * CHeck for metadata
	 * Then we will update the node in db
	 * Inform the octopus service for dict update
	 * Writing the response
	 */

	//getting the app context
	appCtx := ctx.Value(routes.AppContextKey).(*config.AppContext)
	appCtx.Log.Info("Got a request to update the dataset info by", appCtx.Session.User.ID)

	//parse the request param node metadata
	md := []models.NodeMetadata{}
	err := json.NewDecoder(r.Body).Decode(&md)
	if err != nil {
		//bad request
		appCtx.Log.Error("error while parsing the node metadata", err.Error())
		response.WriteError(w, response.Error{Err: "Invalid Params " + err.Error()}, http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	//checking for the metadata
	if len(md) == 0 {
		appCtx.Log.Error("couldn't find any node metadata to update")
		response.WriteError(w, response.Error{Err: "Couldn't find any node metadata to update"}, http.StatusBadRequest)
		return
	}

	//updating the node metadata
	err = models.UpdateNodeMetadata(md, appCtx.Db)
	if err != nil {
		//error while updating the metadata
		appCtx.Log.Error("error while updating the node metadata", err.Error())
		response.WriteError(w, response.Error{Err: "Error while updating the node metadata in db"}, http.StatusInternalServerError)
		return
	}

	//informing the octopus service to update the dict
	//TODO

	//writing the response
	appCtx.Log.Info("Successfully updated the node metadata for the dataset")
	response.Write(w, response.Message{Message: "Successfully updatede the node metadata"})
}
