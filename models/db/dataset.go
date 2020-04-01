// Copyright 2019 Cuttle.ai. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

//Package db contains the db interactions for the models
package db

import (
	"fmt"

	"github.com/cuttle-ai/brain/models"
	"github.com/cuttle-ai/file-uploader-service/config"
	"github.com/cuttle-ai/octopus/interpreter"
	"github.com/google/uuid"
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

//GetColumns get the columns corresponding to a dataset
func (d Dataset) GetColumns(a *config.AppContext) ([]models.Node, error) {
	result := []models.Node{}
	err := a.Db.Set("gorm:auto_preload", true).Where("dataset_id = ? and type = ?", d.ID, interpreter.Column).Find(&result).Error
	return result, err
}

//GetTable get the tables corresponding to a dataset
func (d Dataset) GetTable(a *config.AppContext) (models.Node, error) {
	result := []models.Node{}
	err := a.Db.Set("gorm:auto_preload", true).Where("dataset_id = ? and type = ?", d.ID, interpreter.Table).Find(&result).Error
	if len(result) > 0 {
		return result[0], nil
	}
	return models.Node{}, err
}

//UpdateColumns updates the columns in the database. It will create the columns if not existing
func (d *Dataset) UpdateColumns(a *config.AppContext, cols []models.Node) ([]models.Node, error) {
	/*
	 * We will use the db transactions to start update
	 * If id exists we will update
	 * else we will create the model
	 */
	//starting the transaction
	tx := a.Db.Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()
	if err := tx.Error; err != nil {
		return nil, err
	}

	//will iterate through the cols for create/update
	for i := 0; i < len(cols); i++ {
		cols[i].DatasetID = d.ID
		//if id doesn't exists we will create the node
		if cols[i].ID == 0 {
			cols[i].UID = uuid.New()
			err := tx.Create(&cols[i]).Error
			if err != nil {
				a.Log.Error("error while creating the column node for", cols[i].DatasetID, "at index", i)
				tx.Rollback()
				return nil, err
			}
			continue
		}
		//else we will update the node
		for j := 0; j < len(cols[i].NodeMetadatas); j++ {
			err := tx.Save(&(cols[i].NodeMetadatas[j])).Error
			if err != nil {
				a.Log.Error("error while updating metadata of the column node for", cols[i].ID, cols[i].NodeMetadatas[j].Prop, cols[i].NodeMetadatas[j].ID)
				tx.Rollback()
				return nil, err
			}
		}
	}
	return cols, tx.Commit().Error
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

//Update updates a dataset
func (d *Dataset) Update(a *config.AppContext) error {
	/*
	 * Then we will update
	 */

	//updating the model
	d.UserID = a.Session.User.ID
	err := a.Db.Model(d).Updates(map[string]interface{}{
		"name":        d.Name,
		"description": d.Description,
	}).Error

	return err
}
