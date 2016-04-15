# RMQ_worker
RabbitMQ Consumer in PHP using pcntl.
This Consumer starts N workers and conects them to rabbitmq queues.


###Installation via Composer (Packagist):
1. Include the following in your _composer.json_ file:
```
        "require": {
            ...
            "brutalsys/rmq_worker": "^v1.1"
        }
```
2. Run ```composer install```

##Package Use
###Config File:
You must create a directory ```config``` in your root project folder.
In that folder you should add a ```config.json``` file.
Some examples:
####Declaring a queue
```
{
  "rabbitmq":{
    "host":"127.0.0.1",
    "apiport":15672,
    "user":"test",
    "vhost":"/",
    "password":"test",
    "queues":
    [
      {
        "name":"lvmcloud.hypervisor.wingu1",
        "declare":true,
        "durable":true,
        "exclusive":false,
        "internal":false,
        "auto_delete":false,
        "nowait":true
      }
    ],
    "qos":true
  },
  "daemon":false,
  "shared_memory":{
    "tmp_file":"/tmp/tmp_shm.{n}.tmp"
  },
  "log":[
    {
      "type":"stdout",
      "level":"warning"
    }
  ]
}
```
