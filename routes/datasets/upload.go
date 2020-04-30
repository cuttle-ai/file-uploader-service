// Copyright 2019 Cuttle.ai. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package datasets

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
	"github.com/cuttle-ai/file-uploader-service/models"
	"github.com/cuttle-ai/file-uploader-service/models/db"
	"github.com/cuttle-ai/file-uploader-service/routes"
	routesFile "github.com/cuttle-ai/file-uploader-service/routes/file"
	"github.com/cuttle-ai/file-uploader-service/routes/response"
	"github.com/jinzhu/gorm"
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
	 * Then will start to process the file and store it
	 */

	//getting the app context
	appCtx := ctx.Value(routes.AppContextKey).(*config.AppContext)
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
	f, err := os.Create(newfile)
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
	fT, err := libfile.ProcessFile(newfile, handler.Filename)
	if err != nil {
		//error while identifying the file
		appCtx.Log.Error("error while identifying the file type", newfile, err.Error())
		response.WriteError(w, response.Error{Err: "Unidentified file format"}, http.StatusBadRequest)
		return
	}
	//and store it
	d, err := fT.Store(appCtx)
	if err != nil {
		//error whilen storing the record
		appCtx.Log.Error("error while storing the file type", newfile, err.Error())
		response.WriteError(w, response.Error{Err: "Unidentified file format"}, http.StatusBadRequest)
		return
	}
	fR, _ := d.UploadedDataset.(*models.FileUpload)
	fR.Location = ""
	d.UploadedDataset = fR

	go routesFile.StartPipelineProcess(appCtx, &db.FileUpload{Model: gorm.Model{ID: fR.ID}}, false)

	appCtx.Log.Info("Successfully moved the uploaded file", handler.Filename, "to", newfile, "and stored to db with id", d.ID)
	response.Write(w, response.Message{Message: "Successfully uploaded the file", Data: d})
}

func init() {
	routes.AddRoutes(
		routes.Route{
			Version:     "v1",
			Pattern:     "/upload",
			HandlerFunc: Upload,
		},
	)
}
