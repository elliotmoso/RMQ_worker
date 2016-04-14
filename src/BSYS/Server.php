<?php
    /**
     * Created by BrutalSYS SL.
     * User: ellio
     * Date: 15/04/2016
     * Time: 1:04
     */
    
    namespace BSYS;
    require_once "../../vendor/autoload.php";

    use BSYS\RMQ\Consumer;
    use BSYS\RMQ\SHM;
    use Monolog\Logger;
    use Configula\Config;
    use Monolog\Handler\RotatingFileHandler;
    use Monolog\Handler\ErrorLogHandler;
    use Monolog\Handler\StreamHandler;
    use Monolog\Handler\ElasticSearchHandler;
    use Ulrichsg\Getopt\Argument;
    use Ulrichsg\Getopt\Getopt;
    use Ulrichsg\Getopt\Option;
    /**
     * Class Server
     * @package LVMCE
     * @property Config $config
     * @property Logger $logger
     * @property SHM $shm
     */
    class Server
    {
        const VERSION="1.1.2";
        public $pid;
        public $config;
        public $daemon=false;
        public $shm_tmp_file;

        private $worker_class;
        private $use=[];
        private $shm;
        private $stop_signal=false;
        private $restart=false;
        private $logger;
        private $config_file=false;
        private $in_child=false;
        private $reloads = 0;

        private $console_loglevel=0;
        private $defaults=[
            'rabbitmq'=>[
                'host'=>'localhost',
                'port'=>5672,
                'user'=>'guest',
                'password'=>'guest',
                'vhost'=>'/',
                'queues'=>[
                    [
                        'name'  =>  'test',
                        'declare'=> false,
                        'durable'=> true,
                        'pasive'=>  false
                    ]
                ],
                'exchanges'=>[
                    [
                        'name'=>    'exchange_test',
                        'bind'=>[
                        ],
                        'declare'=> false,
                        'durable'=> true,
                        'auto_delete'=>false,
                        'type'=>    'fanout'
                    ]
                ],
                'consumerTag'=>false,
                'max_workers'=>50,
                'min_workers'=>2,
                'spare_workers'=>5,
                "worker_autoadjust"=>true,
                'autoadjust'=>array(
                    "create"=>90,
                    "destroy"=>10,
                    "time_sample"=>10
                )
            ],
            'loop_usleep'=>1000000,
            'daemon'=>false,
            'user'=>'root',
            'group'=>'root',
            'lock_file'=>'/var/run/RMQ_daemon.lock',
            'pid_file'=>'/var/run/RMQ_daemon.pid',
            'shared_memory'=>[
                'permissions'=>600,
                'tmp_file'=>'/tmp/RMQ_daemon.{n}.tmp',
            ],
            'log_base'=>'RMQ_Daemon',
            'log'=>[
                [
                    'level'=>'info',
                    'type'=>'file',
                    'file'=>'./logs/RMQ.log',
                    'rotate'=>true,
                    'max_logs'=>5,
                ],
                [
                    'type'=>'stdout',
                ]
            ]
        ];
        private $force_add=false;
        private $force_destroy=false;
        private $consumers=[];
        private $consumers_id=[];
        private $RMQ_api;
        private $max_workers=null;
        private $min_workers=1;
        private $worker_limits=array(
            'destroy'=>90,
            'create'=>10,
        );

        static $SIGNALS=[
            SIGTERM =>'SIGTERM',
            SIGQUIT =>'SIGQUIT',
            SIGINT  =>'SIGINT',
            SIGHUP  =>'SIGHUP',
            SIGUSR1 =>'SIGUSR1',
            SIGUSR2 =>'SIGUSR2',
            SIGALRM =>'SIGALRM',
            SIGCHLD =>'SIGCHLD'
        ];
        public function getDefaults(){
            echo json_encode($this->defaults,JSON_PRETTY_PRINT|JSON_UNESCAPED_SLASHES);
            exit(0);
        }
        public function getLogger($name=false){
            if($name){
                return clone $this->logger->withName($name);
            }else{
                return clone $this->logger;
            }
        }
        public function setConfigFile($config_file){
            $this->config_file=$config_file;
        }
        public function setConsoleLogLevel($log_level){
            $this->console_loglevel=$log_level;
        }
        private function getopt(){
            $getopt=new Getopt(
                [
                    (new Option('h','help',Getopt::NO_ARGUMENT))->setDescription('Show this help'),
                    (new Option('d','default',Getopt::NO_ARGUMENT))->setDescription('Get Default config in json mode'),
                    (new Option('r','reload',Getopt::NO_ARGUMENT))->setDescription('Reload workers'),
                    (new Option('D','daemon',Getopt::NO_ARGUMENT))->setDescription('Daemonize'),
                    (new Option('c','config',Getopt::OPTIONAL_ARGUMENT))->setDefaultValue(false)->setDescription('Set custom config file')->setArgument(new Argument(null,null,'file path')),
                    (new Option('v','verbose',Getopt::NO_ARGUMENT))->setDescription('Increase verbosity'),
                    (new Option('V','version',Getopt::NO_ARGUMENT))->setDescription('Get version')
                ]
            );
            $getopt->parse();
            if($getopt->getOption('h')){
                echo $getopt->getHelpText(30);
                exit(0);
            }
            if($getopt->getOption('V')){
                echo "LVMCloud Core Worker\nVersion: ".Server::VERSION.PHP_EOL;
                exit(0);
            }
            if($getopt->getOption('r')){
                $this->getConfig();
                $rel=$this->reload_extern();
                if($rel!==true){
                    echo $rel.PHP_EOL;
                }
                exit(0);
            }
            if($getopt->getOption('D')){
                $this->daemon=true;
            }
            if($getopt->getOption('d')){
                $this->getDefaults();
                exit(0);
            }
            $loglevel=is_null($getopt->getOption('v'))?0:$getopt->getOption('v');
            $config=is_null($getopt->getOption('c')) || is_numeric($getopt->getOption('c'))?false:realpath($getopt->getOption('c'));
            $this->setConsoleLogLevel($loglevel);
            if($config)
                $this->setConfigFile($config);
        }
        public function __construct($daemon=false,$worker_class='\LVMCE\Cloud')
        {
            if(version_compare(phpversion(),'7.0.0','<')){
                echo('Requires PHP7'.PHP_EOL);
                trigger_error("This service requires PHP7",E_USER_ERROR);
            }
            $this->worker_class=$worker_class;
            $this->daemon=$daemon;
            @cli_set_process_title("LVMCloud Server");
            $this->pid=getmypid();
            $this->getopt();
            $this->getConfig();
            $this->RMQ_api=new RMQ\API($this->config);
        }
        private function configureLogger(){
            $this->logger=new Logger($this->config['log_base']);
            foreach($this->config['log'] as $log){
                $level=Logger::NOTICE;
                if(isset($log['level']))
                    $level=defined('Monolog\Logger::'.strtoupper($log['level']))?constant('Monolog\Logger::'.strtoupper($log['level'])):$level;
                switch($log['type']){
                    case 'file':
                        if(!isset($log['rotate']))
                            $log['rotate']=false;
                        if($log['rotate']){
                            if(!isset($log['max_logs']))
                                $log['max_logs']=0;
                            $this->logger->pushHandler(new RotatingFileHandler($log['file'],$log['max_logs'],$level,true,null,true));
                        }else{
                            $this->logger->pushHandler(new StreamHandler($log['file'],$level,true,null,true));
                        }
                        break;
                    case 'elasticsearch':
                        $elastica=new \Elastica\Client(['servers'=>$log['servers']]);
                        $this->logger->pushHandler(new ElasticSearchHandler($elastica,$log['options'],$level));
                        break;
                    case 'stdout':
                    default:
                        $already=false;
                        foreach($this->logger->getHandlers() as $handler){
                            if($handler instanceof ErrorLogHandler){
                                $already=true;
                            }
                        }
                        if(!$this->daemon && !$already){
                            if(600-$this->console_loglevel*100<$level)
                                $level=600-$this->console_loglevel*100;
                            $this->logger->pushHandler(new ErrorLogHandler(ErrorLogHandler::OPERATING_SYSTEM,$level));
                        }
                        break;
                }
            }
            if(!$this->daemon && $this->console_loglevel){
                $already=false;
                foreach($this->logger->getHandlers() as $handler){
                    if($handler instanceof ErrorLogHandler){
                        $already=true;
                    }
                }
                if(!$already){
                    $level=600-$this->console_loglevel*100;
                    $this->logger->pushHandler(new ErrorLogHandler(ErrorLogHandler::OPERATING_SYSTEM,$level));
                }
            }
            set_error_handler([$this,"errorHandler"]);
            register_shutdown_function([$this,"check_for_fatal"]);
        }
        private function daemonize(){
            $user=posix_getpwnam($this->config['user']);
            $group=posix_getgrnam($this->config['group']);
            if(!$user){
                $this->logger->addEmergency("The user ".$this->config['user'].' could not be found');
                exit(1);
            }elseif(!$group){
                $this->logger->addEmergency('The group '.$this->config['group'].' could not be found');
                exit(1);
            }elseif($this->config['user']!=$this->config['group'] && !in_array($user,$group['members'])){
                $this->logger->addEmergency("The user ".$this->config['user'].' is not member of group '.$this->config['group']);
                exit(1);
            }
            $this->logger->addNotice("Daemonizing Server Process");
            $pid=pcntl_fork();
            if($pid){
                pcntl_waitpid(-1,$status,WNOHANG);
                exit(0);
            }else{
                $id=posix_setsid();
                if($id<0){
                    $this->logger->addCritical('Can not create new session ID');
                    exit(1);
                }
                posix_setuid($user['uid']);
                posix_setgid($group['gid']);
                $server=new Server(true,$this->worker_class);
                $server->start();
                exit(0);
            }
        }

        public function __clone(){
            if(isset($this->shm))
                $this->shm->close_exit();
            $this->consumers=[];
            $this->consumers_id=[];
            unset($this->shm);
        }

        private function createConsumer($id=false){
            $pid=pcntl_fork();
            if($pid){
                pcntl_waitpid(-1,$status,WNOHANG);
            }else{
                $this->in_child=true;
                if(!is_null($this->shm))
                    $this->shm->close_exit();
                unset($this->shm);
                $this->consumers=[];
                $this->consumers_id=[];
                pcntl_signal_dispatch();
                new Consumer(clone $this,$id);
                exit(0);
            }
            pcntl_waitpid(-1,$status,WNOHANG);
            $this->configureSignals();
            $this->consumers_id[$pid]=$id;
            return $pid;
        }
        public function getWorkerClass(){
            return $this->worker_class;
        }
        private function configureSignals()
        {
            if (extension_loaded('pcntl')) {
                pcntl_signal(SIGINT,  [$this, 'signalHandler']);
                pcntl_signal(SIGTERM, [$this, 'signalHandler']);
                pcntl_signal(SIGHUP, [$this, 'signalHandler']);
                pcntl_signal(SIGQUIT, [$this, 'signalHandler']);
                pcntl_signal(SIGUSR1, [$this, 'signalHandler']);
                pcntl_signal(SIGUSR2, [$this, 'signalHandler']);
                pcntl_signal(SIGALRM, [$this, 'alarmHandler']);
                pcntl_signal(SIGCHLD, SIG_IGN);
            }
        }
        private function isLocked(){
            if( file_exists( $this->config['lock_file'] ) )
            {
                # check if it's stale
                $lockingPID = trim( file_get_contents( $this->config['lock_file'] ) );
                if (posix_getpgid($lockingPID) !== false) {
                    return true;
                }
                # Lock-file is stale, so kill it.  Then move on to re-creating it.
                $this->logger->addCritical("Lock file exists but process is not running");
                unlink( $this->config['lock_file'] );
            }
            return false;
        }
        public function reload_extern(){
            if(file_exists($this->config['pid_file'])){
                $pid=file_get_contents($this->config['pid_file']);
                if(posix_getpgid($pid) !== false){
                    posix_kill($pid,SIGHUP);
                    return true;
                }else{
                    unlink($this->config['pid_file']);
                    if(file_exists($this->config['lock_file'])){
                        unlink($this->config['lock_file']);
                        return "No server is running, but PID and Lock file exists";
                    }
                    return "No server is running, but PID file exist";
                }
            }else{
                return "No server is running";
            }
        }

        public function start(){
            if(!$this->daemon && $this->config['daemon']){
                if($this->isLocked()){
                    $this->logger->addEmergency("Another server is running");
                    exit(1);
                }
                $this->daemonize();
            }
            if($this->daemon || !$this->config['daemon']){
                file_put_contents( $this->config['lock_file'], $this->pid);
                file_put_contents( $this->config['pid_file'], $this->pid);
            }
            if (extension_loaded('pcntl')) {
                $this->shm=new SHM(clone $this);
                for($i=1;$i<=$this->min_workers;$i++){
                    $this->logger->addInfo("Starting Worker $i");
                    array_push($this->consumers,$this->createConsumer($i));
                }
                $this->configureSignals();
                $this->wait();
            }else{

                if($this->min_workers>1)
                    $this->logger->addWarning("PCNTL NOT ENABLED, ONLY RUNNING ONE WORKER");
                new Consumer($this,'default');
            }
        }
        public function stop(){
            foreach($this->consumers as $consumer){
                posix_kill($consumer,SIGTERM);
                pcntl_waitpid(-1,$status,WNOHANG);
            }
        }
        public function kill(){
            foreach($this->consumers as $consumer){
                posix_kill($consumer,SIGTERM);
                pcntl_waitpid(-1,$status,WNOHANG);
            }
        }
        public function wait_childs(){
            foreach($this->consumers as $key=>$consumer){
                while (posix_getpgid($consumer) !== false) {
                    $this->logger->addInfo("Waiting for Consumer {$this->consumers_id[$consumer]} to shutdown.");
                    usleep(1000000);
                }
                unset($this->consumers[$key]);
                unset($this->consumers_id[$consumer]);
            }
        }
        public function restart(){
            $old_shm=clone $this->shm;
            //$this->shm->close_exit();
            $this->getConfig();
            $this->logger->addNotice("Restarting Workers Gracefully");
            $this->logger->addDebug("COUNT: ".count($this->consumers). " MAX: ".$this->max_workers." MIN: ".$this->min_workers);
            $i=1;
            $this->shm=new SHM(clone $this);
            usleep(100000);
            $old_consumers=$this->consumers;
            $old_consumers_ids=$this->consumers_id;
            foreach($this->consumers as $i=>&$consumer){
                posix_kill($consumer,SIGTERM);
                pcntl_waitpid(-1,$status,WNOHANG);
                if(count($this->consumers)>$this->max_workers){
                    $this->logger->addDebug('Deleting Worker '.$i.' With PID '.$consumer);
                    unset($this->consumers_id[$consumer]);
                    unset($this->consumers[$i]);
                }else{
                    $this->logger->addDebug('Starting Worker '.$i);
                    $consumer=$this->createConsumer($i);
                    $i++;
                }
            }
            if(count($this->consumers)<$this->min_workers){
                for($o=$i;$o<=$this->min_workers;$o++){
                    array_push($this->consumers,$this->createConsumer($o));
                }
            }
            foreach($old_consumers as $key=>$old_consumer){
                while (posix_getpgid($old_consumer) !== false) {
                    $this->logger->addInfo("Waiting for OLD Consumer {$old_consumers_ids[$old_consumer]} to shutdown.");
                    usleep(100000);
                }
            }
            $old_shm->open();
            $old_shm->destroy();
        }

        function __destruct()
        {
            if(!$this->in_child) {
                $this->kill();
                $this->wait_childs();
                if (isset($this->shm)) {
                    $this->logger->addDebug(getmypid() . ' Removing Shared Memory ' . (integer)$this->shm->getRes());
                }
            }
        }

        public function getConfig(){

            $this->config = new Config(null,$this->defaults);
            if($this->config_file){
                $this->config->loadConfgFile($this->config_file);
            }else{
                $this->config->loadConfig("./config/");
            }
            $this->max_workers=$this->config['rabbitmq.max_workers'];
            $this->min_workers=$this->config['rabbitmq.min_workers'];
            if(strpos($this->config['shared_memory.tmp_file'],'{n}')===false){
                $this->shm_tmp_file=$this->config['shared_memory.tmp_file'].$this->reloads;

            }else{
                $this->shm_tmp_file=str_replace('{n}',$this->reloads,$this->config['shared_memory.tmp_file']);
            }
            $this->worker_limits["create"]=$this->config["rabbitmq.autoadjust.create"];
            $this->worker_limits["destroy"]=$this->config["rabbitmq.autoadjust.destroy"];
            $this->reloads++;
            $this->configureLogger();
            return $this->config;
        }

        private function killWorker($pid=null){
            if(is_null($pid)){
                $pid=reset($this->consumers);
            }
            posix_kill($pid,SIGTERM);
            if (($key = array_search($pid, $this->consumers)) !== false) {
                unset($this->consumers_id[$pid]);
                unset($this->consumers[$key]);
            }
        }

        /**
         * Alarm handler
         * @param  int $signalNumber
         * @return void
         */
        public function alarmHandler($signalNumber)
        {
            $this->logger->addDebug("Recieved alarm #".self::$SIGNALS[$signalNumber].'!');
            $this->logger->addInfo('Memory usage: '.memory_get_usage(true));
            return;
        }

        /**
         * Signal handler
         * @param  int $signalNumber
         * @return void
         */
        public function signalHandler($signalNumber)
        {
            switch ($signalNumber) {
                case SIGTERM:  // 15 : supervisor default stop
                case SIGQUIT:  // 3  : kill -s QUIT
                    $this->logger->addNotice("Recieved signal #".self::$SIGNALS[$signalNumber].'! Stopping gracefully');
                    $this->stop();
                    $this->stop_signal=true;
                    break;
                case SIGINT:   // 2  : ctrl+c
                    $this->logger->addNotice("Recieved signal #".self::$SIGNALS[$signalNumber].'! Stopping gracefully');
                    $this->stop();
                    $this->stop_signal=true;
                    break;
                case SIGHUP:   // 1  : kill -s HUP
                    $this->logger->addNotice("Recieved signal #".self::$SIGNALS[$signalNumber].'! Setting up for restart');
                    $this->restart=true;
                    break;
                case SIGUSR1:  // 10 : kill -s USR1
                    $this->logger->addNotice("Recieved signal #".self::$SIGNALS[$signalNumber].'! Setting up for start 1 worker');
                    $this->force_add=true;
                    break;
                case SIGUSR2:  // 12 : kill -s USR2
                    $this->logger->addNotice("Recieved signal #".self::$SIGNALS[$signalNumber].'! Setting up for destroy 1 worker');
                    $this->force_destroy=true;
                    break;
                default:
                    break;
            }
            $this->configureSignals();
            return;
        }

        public function errorHandler($num, $str, $file, $line, $context = array()){
            switch($num){
                case E_WARNING:
                    $this->logger->addWarning(sprintf("%s on %s:%n",$str,$file,$line),$context);
                    break;
                case E_NOTICE:
                    $this->logger->addNotice(sprintf("%s on %s:%n",$str,$file,$line),$context);
                    break;
                case E_STRICT:
                    $this->logger->addDebug(sprintf("%s on %s:%n",$str,$file,$line),$context);
                    break;
                case E_ERROR:
                    $this->logger->addEmergency(sprintf("%s on %s:%n",$str,$file,$line),$context);
                    break;
                default:
                    $this->logger->addInfo(sprintf("%s on %s:%n",$str,$file,$line),$context);
                    break;
            }
            return true;
        }
        public function check_for_fatal(){
            $error = error_get_last();
            if ( $error["type"] == E_ERROR )
                $this->errorHandler( $error["type"], $error["message"], $error["file"], $error["line"] );
        }
        protected function check_workers(){
            foreach($this->consumers as $key=>$consumer){
                if (posix_getpgid($consumer) === false) {
                    $this->logger->addCritical("Consumer {$this->consumers_id[$consumer]} has shutdown. Restarting it");
                    unset($this->consumers[$key]);
                    unset($this->consumers_id[$consumer]);
                    array_push($this->consumers,$this->createConsumer(count($this->consumers)+1));
                }
            }
        }
        public function wait(){
            while(!$this->stop_signal){
                $this->check_workers();
                if($this->restart){
                    $this->restart();
                    $this->restart=false;
                }
                if($this->force_add){
                    array_push($this->consumers,$this->createConsumer(count($this->consumers)+1));
                    $this->force_add=false;
                }
                if($this->force_destroy){
                    $this->killWorker();
                    $this->force_destroy=false;
                }
                $this->configureSignals();
                if($this->config['rabbitmq.worker_autoadjust']){
                    $this->checkQueue();
                }
                usleep($this->config['loop_usleep']);
                pcntl_signal_dispatch();
            }
            $this->wait_childs();
            if($this->daemon || !$this->config['daemon']){
                unlink($this->config['pid_file']);
                unlink($this->config['lock_file']);
            }
        }
        protected function getConsumerUtilization(){
            $use=[];
            foreach($this->consumers_id as $pid=>$id){
                if($this->shm->has_key($id)){
                    $_use=$this->shm->read($id);
                }else{
                    $_use=0;
                }
                $use[]=$_use;
            }
            $suse=(array_sum($use))/count($use);
            array_push($this->use,$suse);
            if(count($this->use) > $this->config['rabbitmq.autoadjust.time_sample']*1000000/$this->config['loop_usleep']){
                array_shift($this->use);
            }
        }
        protected function ConsumerLoad(){
            $this->getConsumerUtilization();
            if(count($this->use) < $this->config['rabbitmq.autoadjust.time_sample']*1000000/$this->config['loop_usleep']){
                return end($this->use)/2*100;
            }
            $load=array_sum($this->use)/count($this->use)*100;
            $this->logger->addDebug("Worker Utilization: $load");
            return $load;
        }
        protected function checkQueue(){
            $consumer_utilization=$this->ConsumerLoad();
            if($consumer_utilization>$this->worker_limits["create"] && count($this->consumers)<$this->max_workers){
                $this->logger->addWarning("Load ($consumer_utilization) greatter than {$this->worker_limits["create"]}, Adding new Worker to pool");
                array_push($this->consumers,$this->createConsumer(count($this->consumers)+1));
            }
            if($consumer_utilization<$this->worker_limits["destroy"] && count($this->consumers)>$this->min_workers){
                $this->logger->addWarning("Load ($consumer_utilization) less than {$this->worker_limits["destroy"]}, Deleting Worker from pool");
                $this->killWorker();
            }
        }
    }