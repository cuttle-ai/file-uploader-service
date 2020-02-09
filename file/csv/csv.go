// Copyright 2019 Cuttle.ai. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

//Package csv has the implementation of the file interface for csv files
package csv

import (
	"os"

	"github.com/Clever/csvlint"
	"github.com/cuttle-ai/file-uploader-service/config"
	"github.com/cuttle-ai/file-uploader-service/models"
	"github.com/cuttle-ai/file-uploader-service/models/db"
)

//CSV handles the fiels of csv type
type CSV struct {
	//Filename is the name of the file
	Filename string
	//Name is the dataset name
	Name string
	//Resource holds the db instance of the underlying file
	Resource db.FileUpload
}

//ID returns the underlying file's id in db
func (c CSV) ID() uint {
	return c.Resource.ID
}

//Store stores the csv info to database
func (c *CSV) Store(a *config.AppContext) (*models.Dataset, error) {
	/*
	 * We will db transaction we have to save the file upload and the dataset info
	 * Then we will create the file upload
	 * Then we will create the dataset with resource id as the of the file
	 */
	//starting the transaction
	tx := a.Db.Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()
	if err := tx.Error; err != nil {
		//error while beginning the transaction
		return nil, err
	}

	//saving the file upload
	fileRecord := &models.FileUpload{Name: c.Name, UserID: a.Session.User.ID, Location: c.Filename, Status: models.FileUploadStatusUploaded, Type: models.FileUploadTypeCSV}
	if err := tx.Create(fileRecord).Error; err != nil {
		//error while creating the upload
		tx.Rollback()
		a.Log.Error("error while creating the file upload record")
		return nil, err
	}

	//saving the dataset record
	dataset := &models.Dataset{Name: c.Name, UserID: fileRecord.UserID, ResourceID: fileRecord.ID, Source: models.DatasetSourceFile}
	if err := tx.Create(dataset).Error; err != nil {
		//error while creating the dataset
		tx.Rollback()
		a.Log.Error("error while creating the datset record")
		return nil, err
	}
	dataset.UploadedDataset = fileRecord

	//returning the result
	return dataset, tx.Commit().Error
}

//Validate will validate the csv file and returns the errors existing while parsing the csv file
func (c *CSV) Validate() ([]error, error) {
	/*
	 * We will open the file
	 * Then we will validate the same
	 * return the errors if any
	 */
	f, err := os.Open(c.Filename)
	if err != nil {
		c.Resource.Status = models.FileUploadStatusValidatingError
		return nil, err
	}
	defer f.Close()
	invalids, _, err := csvlint.Validate(f, rune(','), true)
	if err != nil {
		c.Resource.Status = models.FileUploadStatusValidatingError
		return nil, err
	}
	c.Resource.Status = models.FileUploadStatusValidated
	if len(invalids) == 0 {
		return nil, nil
	}
	errorResults := []error{}
	for _, v := range invalids {
		errorResults = append(errorResults, v)
	}
	return errorResults, nil
}

//Clean will attemnpt to attempt to clean the file and reprt back the errors occurred while cleaning it
func (c *CSV) Clean() []error {
	return nil
}

//Upload will attempt to upload the file to the analytics engine and report any error occurred
func (c *CSV) Upload() error {
	return nil
}

//UpdateStatus updates the status of the file upload in db
func (c *CSV) UpdateStatus(a *config.AppContext) error {
	/*
	 * We will update the status
	 */
	return a.Db.Model(&c.Resource).Updates(map[string]interface{}{
		"status": c.Resource.Status,
	}).Error
}
