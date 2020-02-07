// Copyright 2019 Cuttle.ai. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

//Package models contains the models required by the file uploader service
package models

import (
	"github.com/jinzhu/gorm"
)

const (
	//FileUploadStatusUploaded indicates that the file has been uploaded
	FileUploadStatusUploaded = "UPLOADED"
)

const (
	//FileUploadTypeCSV indicates that the uploaded file's type is csv
	FileUploadTypeCSV = "CSV"
)

//FileUpload represents the file uploads in the system
type FileUpload struct {
	gorm.Model
	//Name of the upload
	Name string
	//UserID is the id of the user with whom the file is associated with
	UserID uint
	//Location is the location where the file is stored
	Location string
	//Type is the type of file uploaded
	Type string
	//Status is the status of the uploaded file
	Status string
}

//FileUploadError stores the errors happened while uploading a file
type FileUploadError struct {
	gorm.Model
	//FileUploadID is the id of the upload
	FileUploadID uint
	//Error is the error associated with the file upload
	Error string
}

//FileDataset has the info about an uploaded datatset and its errors
type FileDataset struct {
	//Info has the info about the dataset
	Info FileUpload
	//Errors has the list errors of the dataset upload
	Errors []FileUploadError
}
