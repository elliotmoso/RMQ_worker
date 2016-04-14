<?php
    /**
     * Created by BrutalSYS SL.
     * User: ellio
     * Date: 06/04/2016
     * Time: 18:25
     */

    namespace BSYS\RMQ;

    use BSYS\Server;
    class SHM
    {
        private $config;
        private $logger;
        private $tmp_file;
        private $shm_res;
        private $child;
        private $server;
        public function __construct(Server $server,$child=false)
        {
            $this->server=$server;
            $this->config=$server->getConfig();
            $this->child=$child;
            $this->tmp_file=$server->shm_tmp_file;
            while(!file_exists($this->tmp_file)){
                touch($this->tmp_file);
                chmod($this->tmp_file,$this->config['shared_memory.permissions']);
            }
            if(!$this->child){
                $this->logger=$server->getLogger($this->config['log_base'].'.SHM');
            }else{
                $this->logger=$server->getLogger($this->config['log_base'].'.workerSHM.'.$child);
            }
            $this->open();
        }

        public function open(){
            $perms=$this->config['shared_memory.permissions'];
            $size=$this->config['rabbitmq.max_workers']*44;
            $file=$this->tmp_file;
            $this->shm_res=shm_attach(ftok($file,'L'),$size,$perms);
            $this->logger->debug('Opening shared memory resource with id: ['.intval($this->shm_res).'] on file ['.$this->tmp_file.']');
        }
        public function remove($key){
            return shm_remove_var($this->shm_res,$key);
        }
        public function close(){
            $this->logger->debug('Closing shared memory resource');
            shm_detach($this->shm_res);
        }
        public function has_key($key){
            return shm_has_var($this->shm_res,$key);
        }
        public function read($key){
            return shm_get_var($this->shm_res,$key);
        }
        public function write($key,$data){
            return shm_put_var($this->shm_res,$key,$data);
        }
        public function close_exit(){
            shm_detach($this->shm_res);
            $this->shm_res=null;
        }
        public function destroy(){
            if(!is_null($this->shm_res) && is_resource($this->shm_res)){
                shm_remove($this->shm_res);
                if(file_exists($this->tmp_file)){
                    unlink($this->tmp_file);
                }
                $this->shm_res=null;
                $this->logger->debug('Delete shared memory resource');
            }
        }
        public function getRes(){
            return $this->shm_res;
        }
        public function __destruct()
        {
            if(!is_null($this->shm_res)){
                if($this->child){
                    $this->close();
                }else{
                    $this->destroy();
                }
            }
        }
        public function __clone(){
            return null;
        }
    }