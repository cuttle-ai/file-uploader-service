// Copyright 2019 Cuttle.ai. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

//Package db contains the db interactions for the models
package db

import (
	"github.com/cuttle-ai/file-uploader-service/config"
	"github.com/cuttle-ai/file-uploader-service/models"
)

//GetFileUploads returns the list of file uploads for a user
func GetFileUploads(a *config.AppContext) ([]models.FileUpload, error) {
	results := []models.FileUpload{}
	err := a.Db.Where("user_id = ?", a.Session.User.ID).Find(&results).Error
	return results, err
}

//GetFileUpload returns the list of file uploads for a user
func GetFileUpload(a *config.AppContext, id int) (models.UploadedDataset, error) {
	result := models.UploadedDataset{}
	err := a.Db.Where("user_id = ? and id = ?", a.Session.User.ID, id).Find(&result.Info).Error
	if err != nil {
		return result, err
	}
	err = a.Db.Where("file_upload_id = ?", id).Find(&result.Errors).Error
	return result, err
}
