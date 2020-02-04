// Copyright 2019 Cuttle.ai. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package routes

/*
 * This file contains the file upload api
 */

import (
	"context"
	"io"
	"net/http"
	"os"
	"os/user"
	"path/filepath"

	"github.com/cuttle-ai/file-uploader-service/config"
	libfile "github.com/cuttle-ai/file-uploader-service/file"
	"github.com/cuttle-ai/file-uploader-service/routes/response"
)

//Upload will upload a file to the platform and will start the process of verifying the file
func Upload(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	/*
	 * We will get the app context
	 * Then we will parse the multipart file
	 * we will get the file
	 * Then we will get the system user home directory
	 * we will create the new directory location where the uploaded file has to be moved
	 * Will create the new file name
	 * Then will move the file into that location
	 * Then will start to process the file
	 */

	//getting the app context
	appCtx := ctx.Value(AppContextKey).(*config.AppContext)
	appCtx.Log.Info("Got a request to upload a file by", appCtx.Session.User.Email)

	//parsing the multipart form
	//maximum we can parse 1Gb file size
	r.ParseMultipartForm(10 << 30)

	//we are getting the file
	file, handler, err := r.FormFile("file")
	if err != nil {
		appCtx.Log.Error("error retrieving the File", err)
		response.WriteError(w, response.Error{Err: "Error while reading the uploaded file"}, http.StatusBadRequest)
		return
	}
	defer file.Close()

	appCtx.Log.Info("A file upload has been initiated", handler.Filename, "of size", handler.Size)

	//we are getting the user home
	usr, err := user.Current()
	if err != nil {
		//error while getting the current user home
		appCtx.Log.Error("error while uploading the user home details while moving the uploaded file", handler.Filename)
		response.WriteError(w, response.Error{Err: "Error while saving the uploaded file to a server location"}, http.StatusInternalServerError)
		return
	}

	//will create the directory if required for the new file
	req := libfile.NewNameGenerate()
	req.Name = usr.HomeDir + config.FileDumpDirectory + appCtx.Session.User.Email
	libfile.GenerateNameChan <- req
	out := <-req.Out
	newpath := out.Generated
	os.MkdirAll(newpath, 0755)

	//creating the new file name
	newfile := newpath + string([]rune{filepath.Separator}) + handler.Filename

	//move the file
	f, err := os.OpenFile(newfile, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		appCtx.Log.Error("error while moving the file to the processing location", handler.Filename, err.Error())
		response.WriteError(w, response.Error{Err: "Error while moving the uploaded file to a server location"}, http.StatusInternalServerError)
		return
	}
	defer f.Close()
	_, err = io.Copy(f, file)
	if err != nil {
		appCtx.Log.Error("error while moving the file to the processing location", handler.Filename, err.Error())
		response.WriteError(w, response.Error{Err: "Error while moving the uploaded file to a server location"}, http.StatusInternalServerError)
		return
	}

	//we will start processing the file
	go libfile.ProcessFile(newfile)

	appCtx.Log.Info("Successfully moved the uploaded file", handler.Filename, "to", newfile)
	response.Write(w, "Successfully uploaded the file")
}

func init() {
	AddRoutes(
		Route{
			Version:     "v1",
			Pattern:     "/upload",
			HandlerFunc: Upload,
		},
	)
}
