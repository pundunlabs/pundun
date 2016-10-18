# pundun

Statistical Data Analysis Framework

### Building Pundun Framework

```sh
$ git clone https://github.com/pundunlabs/pundun.git
$ cd pundun
$ rebar get-deps
$ rebar compile
$ (cd rel && rebar generate)
```

### Configuring Pundun Binary Protocol Server
Edit '<PROD>/lib/pundun<VERSION>/priv/pundun.yaml'; where <PROD> is 'rel/pundun' if above commands are applied.
Modify 'pbp_server_options' parameter.

SSL certificate and key files should be defined here.

To generate self signed certificate files, one may use below commands.

```sh
$ cd <PROD>/lib/pundun<VERSION>/priv/
$ openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 1095
```

### Configuring SHH Daemon>

Edit '<PROD>/lib/pundun<VERSION>/priv/pundun.yaml'.
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
Optionally store any public key in '<PROD>/lib/pundun<VERSION>/priv/<system_dir>/authorized_keys' file.

### Initial configuration of the sytem and starting the node

```sh
$ cd <PROD>/bin/
$ ./pundun configure
$ ./pundun start
```
Read local logs from '<PROD>/log/local.pundun.log' file.

### Connecting to Command Line Interface
To connect local pundun node's CLI that is created as above.
```sh
$ ssh localhost -p 8989
```
Or ssh to remote node that listens on a configured ip and port.
