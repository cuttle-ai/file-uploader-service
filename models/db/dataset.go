// Copyright 2019 Cuttle.ai. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

//Package db contains the db interactions for the models
package db

import (
	"errors"
	"fmt"

	authConfig "github.com/cuttle-ai/auth-service/config"
	"github.com/cuttle-ai/brain/models"
	"github.com/cuttle-ai/file-uploader-service/config"
	fModels "github.com/cuttle-ai/file-uploader-service/models"
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

//Dataset is the type alias for models.Dataset
type Dataset models.Dataset

//DatsetUserMapping has mappings of user to datasets
type DatsetUserMapping models.DatsetUserMapping

//Get returns the info about a dataset including the uploaded resource info in Uploaded dataset
func (d *Dataset) Get(a *config.AppContext, maskSensitiveInfo bool) error {
	/*
	 * First we will get the dataset
	 * Then based on the source, we will get the resource
	 */
	//getting the dataset
	ds := models.Dataset(*d)
	err := (&ds).Get(a.Db)
	if err != nil {
		return err
	}
	*d = Dataset(ds)

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

//GetColumns get the columns corresponding to a dataset
func (d Dataset) GetColumns(a *config.AppContext) ([]models.Node, error) {
	ds := models.Dataset(d)
	return ds.GetColumns(a.Db)
}

//GetTable get the tables corresponding to a dataset
func (d Dataset) GetTable(a *config.AppContext) (models.Node, error) {
	ds := models.Dataset(d)
	return ds.GetTable(a.Db)
}

//UpdateColumns updates the columns in the database. It will create the columns if not existing
func (d *Dataset) UpdateColumns(a *config.AppContext, cols []models.Node) ([]models.Node, error) {
	ds := models.Dataset(*d)
	res, err := (&ds).UpdateColumns(a.Log, a.Db, cols)
	*d = Dataset(ds)
	return res, err
}

//CreateTable creates the table for the given dataset
func (d *Dataset) CreateTable(a *config.AppContext, table models.Node) (models.Node, error) {
	err := a.Db.Create(&table).Error
	return table, err
}

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

//DeleteSanityCheck will check whether the user has correct access to delete the dataset
func (d Dataset) DeleteSanityCheck(a *config.AppContext) error {
	if a.Session.User.ID != d.UserID && a.Session.User.UserType != authConfig.AdminUser && a.Session.User.UserType != authConfig.SuperAdmin {
		return errors.New("user is not previleged to delete the dataset")
	}
	return nil
}

//Update updates a dataset
func (d *Dataset) Update(a *config.AppContext) error {
	/*
	 * Then we will update
	 */

	//updating the model
	d.UserID = a.Session.User.ID
	err := a.Db.Model(d).Updates(map[string]interface{}{
		"name":          d.Name,
		"description":   d.Description,
		"table_created": d.TableCreated,
		"datastore_id":  d.DatastoreID,
	}).Error

	return err
}

//Delete will delete a given dataset and all the cascaded information
// - nodes
// - node metadata
// - uploaded file
// - dataset
func (d *Dataset) Delete(a *config.AppContext) error {
	/*
	 * We will start the transaction
	 * We will remove the nodes
	 * We will remove the node metadata
	 * We will remove the file upload errors
	 * We will remove the uploaded file info if any
	 * We will delete the dataset user mappings
	 * dataset info
	 * We will commit the changes
	 */
	//starting the transaction
	tx := a.Db.Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()
	if err := tx.Error; err != nil {
		return err
	}

	//deleting the nodes
	err := tx.Where("dataset_id = ?", d.ID).Delete(&models.Node{}).Error
	if err != nil {
		//error while deleting the nodes
		a.Log.Error("error while deleting the nodes associated with the dataset")
		tx.Rollback()
		return err
	}

	//deleting the node metadata
	err = tx.Where("dataset_id = ?", d.ID).Delete(&models.NodeMetadata{}).Error
	if err != nil {
		//error while deleting the node metadata
		a.Log.Error("error while deleting the node metadata associated with the dataset")
		tx.Rollback()
		return err
	}

	//deleting the file uploads errors if any
	err = tx.Where("file_upload_id = ?", d.ResourceID).Delete(&fModels.FileUploadError{}).Error
	if err != nil {
		//error while deleting the file upload errors
		a.Log.Error("error while deleting the file upload errors associated with the dataset")
		tx.Rollback()
		return err
	}

	//deleting the file uploads if any
	err = tx.Where("id = ?", d.ResourceID).Delete(&FileUpload{}).Error
	if err != nil {
		//error while deleting the file uploads
		a.Log.Error("error while deleting the file uploads associated with the dataset")
		tx.Rollback()
		return err
	}

	//deleting the datset user mappings
	err = tx.Where("dataset_id = ?", d.ID).Delete(&DatsetUserMapping{}).Error
	if err != nil {
		//error while deleting the user mappings
		a.Log.Error("error while deleting the user mappings associated with the dataset")
		tx.Rollback()
		return err
	}

	//deleting the dataset
	err = tx.Where("id = ?", d.ID).Delete(&Dataset{}).Error
	if err != nil {
		//error while deleting the dataset
		a.Log.Error("error while deleting the dataset associated with the dataset")
		tx.Rollback()
		return err
	}

	//commiting everything
	return tx.Commit().Error
}

//Create will create a user dataset mapping
func (du *DatsetUserMapping) Create(a *config.AppContext) error {
	return a.Db.Create(du).Error
}

//Update will update a user dataset mapping
func (du *DatsetUserMapping) Update(a *config.AppContext) error {
	return a.Db.Model(du).Updates(map[string]interface{}{"access_type": du.AccessType}).Error
}

//Delete will delete a user dataset mapping
func (du *DatsetUserMapping) Delete(a *config.AppContext) error {
	return a.Db.Delete(du).Error
}
