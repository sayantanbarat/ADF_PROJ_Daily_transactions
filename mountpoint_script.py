dbutils.fs.mount(source='wasbs://<container_name>@<storage_ac_name>.blob.core.windows.net',
                        mount_point='<mount-point-name>',extra_config={'fs.azure.sas.<container_name>.<storage_ac_name>.
                        blob.core.windows.net':'<sas_token>'})