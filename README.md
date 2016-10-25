# pundun

Statistical Data Analysis Framework

### Building Pundun Framework
One may like to change configuration parameters before release. Please see below for pundun configuration.

#####dev-mode
```sh
$ git clone https://github.com/pundunlabs/pundun.git
$ cd pundun
$ rebar3 release
```

#####deployable
```sh
$ rebar3 as prod tar
```

#####deployable with erts included
```sh
$ rebar3 as target tar
```

### Configuring Pundun Binary Protocol Server
Edit 'PROD/etc/pundun.yaml';
    where PROD is '\_build/RELEASE/rel/pundun' and
	  RELEASE is default | prod | target
    if above commands are applied.

Modify 'pbp_server_options' parameter.

SSL certificate and key files should be defined here.

To generate self signed certificate files, one may use below commands.

```sh
$ cd <PROD>/lib/pundun<VERSION>/priv/
$ openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 1095
```

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

### Initial configuration of the sytem and starting the node

```sh
$ cd <PROD>/bin/
$ ./pundun start
```
Read local logs from 'PROD/log/local.pundun.log' file.

### Connecting to Command Line Interface
To connect local pundun node's CLI that is created as above.
```sh
$ ssh localhost -p 8989
```
Or ssh to remote node that listens on a configured ip and port.
