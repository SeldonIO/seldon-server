include settings

dummy:
	@echo dummy

status:
	vagrant global-status

list_boxes:
	vagrant box list

start_env:
	vagrant up

stop_env:
	vagrant halt

destroy_env:
	vagrant destroy

connect_ssh:
	vagrant ssh

reload_provision:
	vagrant reload --provision

build:
	@make clean
	./generate_build_details
	./pre-build/build-seldon-projects
	cp -Rv ../../install-deps install-deps
	make start_env
	./update_vm
	@make package
	@make install_box

clean:
	vagrant destroy -f
	@rm -rfv pre-build/seldon-server-*.war
	@rm -rfv pre-build/seldon-spark-*-with-dependencies.jar
	@rm -rfv install-deps
	rm -fv $(VMBOX_NAME).box
	rm -fv $(VMBOX_NAME).box.sha256
	rm -fv ${VMBOX_NAME}_vagrant.json
	rm -fv end-user-files/Vagrantfile
	rm -fv build-details.txt

setup_for_seldontestvm:
	@cp -v settings.seldontestvm settings
	@cp -v update_vm.seldontestvm update_vm

setup_for_seldonvm:
	@cp -v settings.seldonvm settings
	@cp -v update_vm.seldonvm update_vm

package:
	rm -fv $(VMBOX_NAME).box
	vagrant package --output $(VMBOX_NAME).box
	cat $(VMBOX_NAME).box|shasum -b -a 256 > $(VMBOX_NAME).box.sha256
	./generate_end_user_vagrant_file
	./generate_vagrant_json

remove_box:
	vagrant box remove $(VMBOX_NAME) || :
	vagrant box list

install_box:
	@make remove_box
	vagrant box add --name $(VMBOX_NAME) $(VMBOX_NAME).box
	vagrant box list

show_details:
	@echo "VMBOX_NAME[$(VMBOX_NAME)]"
	@echo "VMBOX_VERSION[$(VMBOX_VERSION)]"
	@echo "VMBOX_DESCRIPTION[$(VMBOX_DESCRIPTION)]"
	@echo "VMBOX_S3_BUCKET[$(VMBOX_S3_BUCKET)]"
	@echo "VMBOX_URL_HOST[$(VMBOX_URL_HOST)]"
	@echo "VMBOX_URL_DIR[$(VMBOX_URL_DIR)]"
	@echo "VMBOX_URL[$(VMBOX_URL)]"

upload_to_s3:
	aws s3 cp $(VMBOX_NAME)_vagrant.json s3://$(VMBOX_S3_BUCKET)/$(VMBOX_NAME)/$(VMBOX_VERSION)/ --acl public-read
	aws s3 cp build-details.txt s3://$(VMBOX_S3_BUCKET)/$(VMBOX_NAME)/$(VMBOX_VERSION)/ --acl public-read
	aws s3 cp $(VMBOX_NAME).box			 s3://$(VMBOX_S3_BUCKET)/$(VMBOX_NAME)/$(VMBOX_VERSION)/ --acl public-read
	make publish_as_latest

publish_as_latest:
	aws s3 cp $(VMBOX_NAME)_vagrant.json s3://$(VMBOX_S3_BUCKET)/$(VMBOX_NAME)/ --acl public-read
	aws s3 cp build-details.txt 	     s3://$(VMBOX_S3_BUCKET)/$(VMBOX_NAME)/ --acl public-read
	aws s3 cp end-user-files/Vagrantfile s3://$(VMBOX_S3_BUCKET)/$(VMBOX_NAME)/ --acl public-read

