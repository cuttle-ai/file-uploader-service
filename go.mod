module github.com/cuttle-ai/file-uploader-service

go 1.13

replace github.com/cuttle-ai/auth-service => ../auth-service/

require (
	github.com/cuttle-ai/auth-service v0.0.0-00010101000000-000000000000
	github.com/cuttle-ai/configs v0.0.0-20190824112953-7860fdfd0dae
	github.com/google/uuid v1.1.1
	github.com/hashicorp/consul/api v1.3.0
	github.com/jinzhu/gorm v1.9.12
	github.com/revel/config v0.21.0 // indirect
	github.com/revel/log15 v2.11.20+incompatible // indirect
	github.com/revel/pathtree v0.0.0-20140121041023-41257a1839e9 // indirect
	github.com/revel/revel v0.21.0
	github.com/twinj/uuid v1.0.0
)
