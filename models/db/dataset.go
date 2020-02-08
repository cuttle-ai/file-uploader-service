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

const (
	//DatasetNameMaxLen is the max length for name field of the dataset
	DatasetNameMaxLen = 500
	//DatasetDescriptionMaxLen is the max length for the description field of dataset
	DatasetDescriptionMaxLen = 500
)

//GetDatasets returns the list of datasets uploads for a user
func GetDatasets(a *config.AppContext) ([]models.Dataset, error) {
	results := []models.Dataset{}
	err := a.Db.Where("user_id = ?", a.Session.User.ID).Find(&results).Error
	return results, err
}

//GetFileUpload returns the info about a fileupload for the given id
func GetFileUpload(a *config.AppContext, id uint, maskSensitiveInfo bool) (models.FileDataset, error) {
	result := models.FileDataset{}
	err := a.Db.Where("user_id = ? and id = ?", a.Session.User.ID, id).Find(&result.Info).Error
	if err != nil {
		return result, err
	}
	err = a.Db.Where("file_upload_id = ?", id).Find(&result.Errors).Error
	if maskSensitiveInfo {
		result.Info.Location = ""
	}
	return result, err
}

//Get returns the info about a dataset including the uploaded resource info in Uploaded dataset
func (d *Dataset) Get(a *config.AppContext, maskSensitiveInfo bool) error {
	/*
	 * First we will get the dataset
	 * Then based on the source, we will get the resource
	 */
	//getting the dataset
	err := a.Db.Where("user_id = ? and id = ?", a.Session.User.ID, d.ID).Find(d).Error
	if err != nil {
		return err
	}

	//based on the source getting the resource
	if d.Source == models.DatasetSourceFile {
		//it is a csv
		f, err := GetFileUpload(a, d.ResourceID, maskSensitiveInfo)
		if err != nil {
			//error while getting the file upload resource
			a.Log.Error("error while getting the resource information for the dataset", d.ID)
			return err
		}
		d.UploadedDataset = f
		return nil
	}

	//haven't found a resource till now
	return fmt.Errorf("couldn't resolve the dataset source for the dataset %s %d", d.Source, d.ID)
}

//Dataset is the type alias for models.Dataset
type Dataset models.Dataset

//UpdateSanityCheck will check whether the dataset has correct values or not
func (d Dataset) UpdateSanityCheck(a *config.AppContext) error {
	if len(d.Name) > DatasetNameMaxLen {
		return fmt.Errorf("maximum allowed length for name is %d. Got %d", DatasetNameMaxLen, len(d.Name))
	}
	if len(d.Description) > DatasetDescriptionMaxLen {
		return fmt.Errorf("maximum allowed length for description is %d. Got %d", DatasetDescriptionMaxLen, len(d.Description))
	}
	return nil
}

//Update updates a dataset
func (d *Dataset) Update(a *config.AppContext) error {
	/*
	 * Then we will update
	 */

	//updating the model
	err := a.Db.Model(d).Updates(map[string]interface{}{
		"name":        d.Name,
		"description": d.Description,
	}).Error

	return err
}
