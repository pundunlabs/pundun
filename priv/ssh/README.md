### Configuring SHH Daemon>

Edit 'PROD/etc/pundun.yaml'.
Modify 'pundun_cli_options' parameter.
Under specified 'user_dir', place public and private keys for ssh client.

```sh
$ cd <PROD>/lib/pundun<VERSION>/priv/ssh
$ ssh-keygen -t rsa -f <user_dir>/id_rsa
```

Under specified 'system_dir', place public and private keys for ssh host.

```sh
$ ssh-keygen -t rsa -f <system_dir>/ssh_host_rsa_key
```
Store any public key in '<PROD>/lib/pundun<VERSION>/priv/<system_dir>/authorized_keys' file.
