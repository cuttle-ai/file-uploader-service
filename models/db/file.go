// Copyright 2019 Cuttle.ai. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

//Package db contains the db interactions for the models
package db

import (
	"github.com/cuttle-ai/file-uploader-service/config"
	"github.com/cuttle-ai/file-uploader-service/models"
)

//FileUpload is the type alias for models.FileUpload
type FileUpload models.FileUpload

//GetFileUpload returns the info about a fileupload for the given id with error details
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

//Get returns the upload file info from the database
func (f *FileUpload) Get(a *config.AppContext) error {
	return a.Db.Where("user_id = ? and id = ?", a.Session.User.ID, f.ID).Find(&f).Error
}

//DeleteErrors will delete the errors for the given file upload
func (f FileUpload) DeleteErrors(a *config.AppContext) error {
	return a.Db.Where("file_upload_id = ?", f.ID).Delete(&models.FileUploadError{}).Error
}

//DeleteErrorsAndUpdateStatus will delete the file upload errors and update the status as uploaded
func (f *FileUpload) DeleteErrorsAndUpdateStatus(a *config.AppContext) error {
	/*
	 * We will start the transaction
	 * We will then delete the file upload errors
	 * Then we will update the status of the file upload as status
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
		return err
	}

	//deleting the errors
	if err := tx.Where("file_upload_id = ?", f.ID).Delete(&models.FileUploadError{}).Error; err != nil {
		//error while creating the upload
		tx.Rollback()
		a.Log.Error("error while deleting the file upload errors for", f.ID)
		return err
	}

	//updating the status
	status := map[string]interface{}{
		"status": models.FileUploadStatusUploaded,
	}
	if err := tx.Model(f).Updates(status).Error; err != nil {
		//error while creating the upload
		tx.Rollback()
		a.Log.Error("error while updating the file upload status to updating for", f.ID)
		return err
	}
	return tx.Commit().Error
}

//CreateErrors will create the error record for the given file
func CreateErrors(a *config.AppContext, errs []models.FileUploadError) error {
	/*
	 * We will db transaction we have to save the file upload errors
	 * Then we will create the file upload error one by one
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
		return err
	}

	//we will iterate and create the records
	for _, v := range errs {
		if err := tx.Create(&v).Error; err != nil {
			//error while creating the dataset
			tx.Rollback()
			a.Log.Error("error while creating the file upload error")
			return err
		}
	}

	return tx.Commit().Error
}
