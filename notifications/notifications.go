// Copyright 2019 Cuttle.ai. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

//Package notifications contains the utilities for sending notitications to the frontend clients
package notifications

import (
	"fmt"

	"github.com/cuttle-ai/brain/models"
	"github.com/cuttle-ai/file-uploader-service/config"
	"github.com/cuttle-ai/go-sdk/services/websockets"
)

func sendNotification(appCtx *config.AppContext, n models.Notification) error {
	return websockets.SendInfoNotification(appCtx, n)
}

//SendValidatedDoneStatus will send the file validation status to the users frontend client.
// done parameter should give the number of records that are validated
func SendValidatedDoneStatus(appCtx *config.AppContext, done int, documentName string) {
	payload := fmt.Sprintf("validated %d records in %s", done, documentName)
	err := sendNotification(appCtx, models.Notification{Payload: payload})
	if err != nil {
		//error while sending websocket notitication to user's client
		appCtx.Log.Error("error while sending validation done status to users' frontend client", err)
	}
}

//SendProcessedStatus will send the file processed status to the users frontend client.
// done parameter should give the number of records that are processed
func SendProcessedStatus(appCtx *config.AppContext, done float32, documentName string) {
	payload := fmt.Sprintf("processed %.f records in %s", done, documentName)
	err := sendNotification(appCtx, models.Notification{Payload: payload})
	if err != nil {
		//error while sending websocket notitication to user's client
		appCtx.Log.Error("error while sending processed status to users' frontend client", err)
	}
}

//SendInfoMessage will send info messages to the users's frontend client
func SendInfoMessage(appCtx *config.AppContext, message string) {
	err := sendNotification(appCtx, models.Notification{Payload: message})
	if err != nil {
		//error while sending websocket notitication to user's client
		appCtx.Log.Error("error while sending info message to users' frontend client", err)
	}
}

//SendErrorMessage will send error messages to the users's frontend client
func SendErrorMessage(appCtx *config.AppContext, message string) {
	err := websockets.SendErrorNotification(appCtx, models.Notification{Payload: message})
	if err != nil {
		//error while sending websocket notitication to user's client
		appCtx.Log.Error("error while sending error message to users' frontend client", err)
	}
}

//SendSuccessMessage will send success messages to the users's frontend client
func SendSuccessMessage(appCtx *config.AppContext, message string) {
	err := websockets.SendSuccessNotification(appCtx, models.Notification{Payload: message})
	if err != nil {
		//error while sending websocket notitication to user's client
		appCtx.Log.Error("error while sending error message to users' frontend client", err)
	}
}
