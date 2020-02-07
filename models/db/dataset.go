// Copyright 2019 Cuttle.ai. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

//Package db contains the db interactions for the models
package db

import (
	"fmt"

	"github.com/cuttle-ai/file-uploader-service/config"
	"github.com/cuttle-ai/file-uploader-service/models"
)

//GetDatasets returns the list of datasets uploads for a user
func GetDatasets(a *config.AppContext) ([]models.Dataset, error) {
	results := []models.Dataset{}
	err := a.Db.Where("user_id = ?", a.Session.User.ID).Find(&results).Error
	return results, err
}

//GetFileUpload returns the info about a fileupload for the given id
func GetFileUpload(a *config.AppContext, id uint) (models.FileDataset, error) {
	result := models.FileDataset{}
	err := a.Db.Where("user_id = ? and id = ?", a.Session.User.ID, id).Find(&result.Info).Error
	if err != nil {
		return result, err
	}
	err = a.Db.Where("file_upload_id = ?", id).Find(&result.Errors).Error
	return result, err
}

//GetDataset returns the info about a dataset including the uploaded resource info in Uploaded dataset
func GetDataset(a *config.AppContext, id int) (*models.Dataset, error) {
	/*
	 * First we will get the dataset
	 * Then based on the source, we will get the resource
	 */
	result := &models.Dataset{}
	//getting the dataset
	err := a.Db.Where("user_id = ? and id = ?", a.Session.User.ID, id).Find(result).Error
	if err != nil {
		return result, err
	}

	//based on the source getting the resource
	if result.Source == models.DatasetSourceFile {
		//it is a csv
		f, err := GetFileUpload(a, result.ResourceID)
		if err != nil {
			//error while getting the file upload resource
			a.Log.Error("error while getting the resource information for the dataset", result.ID)
			return nil, err
		}
		result.UploadedDataset = f
		return result, nil
	}

	//haven't found a resource till now
	return nil, fmt.Errorf("couldn't resolve the dataset source for the dataset %s %d", result.Source, result.ID)
}
