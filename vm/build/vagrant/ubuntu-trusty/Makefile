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
	@rm -rfv bootstrap_scripts/
	cp -Rv ../../install-deps bootstrap_scripts
	vagrant destroy -f
	@make start_env
	vagrant ssh -c '/vagrant/install-deps'

