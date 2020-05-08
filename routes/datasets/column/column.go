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
	"github.com/cuttle-ai/go-sdk/services/octopus"
)

//UpdateNodeMetadata updates the given node metadata in database and inform the octopus service to update the dict
func UpdateNodeMetadata(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	/*
	 * We will get the app context
	 * Then we will parse the node metadata
	 * Check the validaity of metadata
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

	//checking validity of the metadata
	if len(md) == 0 {
		appCtx.Log.Error("couldn't find any node metadata to update")
		response.WriteError(w, response.Error{Err: "Couldn't find any node metadata to update"}, http.StatusBadRequest)
		return
	}
	//checking whether the user has access to the metadata given for update
	datasets := map[uint]struct{}{}
	datasetIds := []uint{}
	for _, v := range md {
		if v.DatasetID == 0 {
			//we can't have datsets with 0 id
			appCtx.Log.Error("couldn't find any node metadata with dataset id 0 for metadata id", v.ID)
			response.WriteError(w, response.Error{Err: "Couldn't find any node metadata with dataset id 0"}, http.StatusBadRequest)
			return
		}
		if _, ok := datasets[v.DatasetID]; !ok {
			datasets[v.DatasetID] = struct{}{}
			datasetIds = append(datasetIds, v.DatasetID)
		}
	}
	ok, err := models.HasUserAccess(appCtx.Log, appCtx.Db, datasetIds, appCtx.Session.User.ID)
	if !ok {
		//user doesn't have access to the datasets to udate the metadata
		appCtx.Log.Error("user doesn't have access to the datasets to udate the metadata", datasetIds, appCtx.Session.User.ID)
		response.WriteError(w, response.Error{Err: "You don't access to the datasets"}, http.StatusForbidden)
		return
	}
	if err != nil {
		//error while checking the access rights
		appCtx.Log.Error("error while checking the access rights of the user to the datasets", datasetIds, appCtx.Session.User.ID, err)
		response.WriteError(w, response.Error{Err: "Error while validating the access rights"}, http.StatusInternalServerError)
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
	err = octopus.UpdateDict(appCtx.Log, config.DiscoveryURL, config.DiscoveryToken, appCtx.Session.ID)
	if err != nil {
		//error while updating the dict from octopus
		appCtx.Log.Error("error while updating the dict from the octopus service for user", appCtx.Session.User.ID, err)
		return
	}

	//writing the response
	appCtx.Log.Info("Successfully updated the node metadata for the dataset")
	response.Write(w, response.Message{Message: "Successfully updatede the node metadata"})
}

func init() {
	routes.AddRoutes(
		routes.Route{
			Version:     "v1",
			Pattern:     "/datasets/nodemetadata/update",
			HandlerFunc: UpdateNodeMetadata,
		},
	)
}
