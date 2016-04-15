<?php
    /**
     * Created by BrutalSYS SL.
     * User: ellio
     * Date: 08/03/2016
     * Time: 15:01
     */

    namespace BSYS\RMQ;
    declare(ticks = 1);
    use PhpAmqpLib\Connection\AMQPStreamConnection;
    use PhpAmqpLib\Message\AMQPMessage;
    use BSYS\Server;

    /**
     * Class Consumer
     * @package BSYS\RMQ
     * @property AMQPStreamConnection $RMQ_conn
     * @property \PhpAmqpLib\Channel\AMQPChannel $RMQ_channel
     * @property AMQPMessage $message
     * @property \Monolog\Logger $logger
     */
    class Consumer
    {
        private $server;
        private $message;
        private $config;
        private $shm;
        private $RMQ_conn;
        private $RMQ_channel;
        private $id=null;
        private $stop_signal=false;
        private $worker_class;
        private $logger;
        public function __construct(Server $server,$id=false)
        {
            $this->worker_class=$server->getWorkerClass();
            $this->shm=new SHM($server,$id);
            $this->shm->write($id,false);
            $this->id=$id?:getmypid();
            $this->server=$server;
            $this->config=clone $server->config;
            $this->configureLogger();
            if(extension_loaded('pcntl'))
                @cli_set_process_title("RMQ Worker [Master: {$server->pid}]");
            $this->configureSignals();
            if(extension_loaded('pcntl'))
                pcntl_signal_dispatch();

            set_error_handler([$this,"errorHandler"]);
            register_shutdown_function([$this,"check_for_fatal"]);
            $this->start();
        }
        public function getConfig(){
            return $this->config;
        }
        public function getLogger(){
            return $this->logger;
        }

        private function configureLogger(){
            $this->logger=$this->server->getLogger($this->config['log_base'].'.worker.'.$this->id);
        }

        private function configureSignals(){
            if (extension_loaded('pcntl')) {
                if (!defined('AMQP_WITHOUT_SIGNALS'))
                    define('AMQP_WITHOUT_SIGNALS', false);
                    pcntl_signal(SIGUSR1, [&$this, 'signalHandler']);
                    pcntl_signal(SIGTERM, [&$this, 'signalHandler']);
                    pcntl_signal(SIGHUP, [&$this, 'signalHandler']);
                    pcntl_signal(SIGINT, [&$this, 'signalHandler']);
                    pcntl_signal(SIGQUIT, [&$this, 'signalHandler']);
                    pcntl_signal(SIGUSR2, [&$this, 'signalHandler']);
                    pcntl_signal(SIGALRM, [&$this, 'alarmHandler']);
                    pcntl_sigprocmask(SIG_BLOCK, array(SIGHUP));

            }
        }
        /**
         * Tell the server you are going to stop consuming
         * It will finish up the last message and not send you any more
         */
        public function stop()
        {
            $this->logger->addDebug('Stopping consumer by cancel command.',array('worker'=>$this->id));
            // this gets stuck and will not exit without the last two parameters set
            //$this->RMQ_channel->basic_cancel($this->config['rabbitmq.consumerTag'], false, true);
        }

        /**
         * Close the channel to the server
         */
        public function stopSoft()
        {
            $this->logger->addDebug('Stopping consumer by closing channel.',array('worker'=>$this->id));
            $this->RMQ_channel->close();
        }

        /**
         * Close the connection to the server
         */
        public function stopHard()
        {
            $this->logger->addDebug('Stopping consumer by closing connection.',array('worker'=>$this->id));
            $this->RMQ_conn->close();
        }
        public function start()
        {
            $this->logger->addNotice('Starting Worker with PID ' . getmypid(), array('worker' => $this->id));
            $this->RMQ_conn = new AMQPStreamConnection(
                $this->config['rabbitmq.host'],
                $this->config['rabbitmq.port'],
                $this->config['rabbitmq.user'],
                $this->config['rabbitmq.password'],
                $this->config['rabbitmq.vhost']
            );
            $this->RMQ_channel = $this->RMQ_conn->channel();
            $temp_queues=[];
            foreach ($this->config['rabbitmq.queues'] as $queue) {
                if (isset($queue['declare']) && $queue['declare']) {
                    $name = $queue['name'];
                    $passive = isset($queue['passive']) ? $queue['passive'] : false;
                    $durable = isset($queue['durable']) ? $queue['durable'] : false;
                    $exclusive = isset($queue['exclusive']) ? $queue['exclusive'] : false;
                    $auto_delete = isset($queue['auto_delete']) ? $queue['auto_delete'] : true;
                    $nowait = isset($queue['nowait']) ? $queue['nowait'] : true;
                    $arguments = isset($queue['arguments']) ? $queue['arguments'] : null;
                    $ticket = isset($queue['ticket']) ? $queue['ticket'] : null;
                    $this->RMQ_channel->queue_declare($name, $passive, $durable, $exclusive, $auto_delete, $nowait, $arguments, $ticket);
                }
            }
            if (isset($this->config['rabbitmq.exchanges']) && is_array($this->config['rabbitmq.exchanges'])) {
                foreach ($this->config['rabbitmq.exchanges'] as $exchange) {
                    if (isset($exchange['declare']) && $exchange['declare']) {
                        $name = $exchange['name'];
                        $type = isset($exchange['type']) ? $exchange['type'] : 'fanout';
                        $passive = isset($exchange['passive']) ? $exchange['passive'] : false;
                        $durable = isset($exchange['durable']) ? $exchange['durable'] : false;
                        $auto_delete=isset($exchange['auto_delete'])?$exchange['auto_delete']:false;
                        $internal   =isset($exchange['internal'])?$exchange['internal']:false;
                        $nowait     =isset($exchange['nowait'])?$exchange['nowait']:true;
                        $arguments  =isset($exchange['arguments'])?$exchange['arguments']:null;
                        $ticket     =isset($exchange['ticket'])?$exchange['ticket']:null;
                        $this->RMQ_channel->exchange_declare($name, $type, $passive, $durable, $auto_delete,$internal,$nowait,$arguments,$ticket);
                        if(isset($exchange['bind']) && is_array($exchange['bind'])){
                            foreach($exchange['bind'] as $bind){
                                $routing_key=isset($bind['routing_key'])?$bind['routing_key']:'';
                                $b_nowait=isset($bind['nowait'])?$bind['nowait']:true;
                                $b_arguments = isset($bind['arguments'])?$bind['arguments']:null;
                                $b_ticket   =isset($bind['ticket'])?$bind['ticket']:null;
                                if(isset($bind['name'])){
                                    $queue=$bind['name'];
                                }else{
                                    list($queue, ,)=$this->RMQ_channel->queue_declare();
                                    $this->logger->addDebug("Added temporally queue $queue");
                                    array_push($temp_queues,[
                                        'name'=>$queue,
                                        'routing_key'=>$routing_key,
                                        'nowait'=>$nowait,
                                        'arguments'=>$arguments,
                                        'ticket'=>$ticket
                                    ]);
                                }
                                $this->RMQ_channel->queue_bind($queue, $name,$routing_key,$b_nowait,$b_arguments,$b_ticket);
                            }
                        }
                    }
                }
            }
            if(isset($this->config['rabbitmq.qos'])){
                if(is_bool($this->config['rabbitmq.qos']) || is_numeric($this->config['rabbitmq.qos'])){
                    $this->RMQ_channel->basic_qos(null, 1, false);
                }elseif(is_array($this->config['rabbitmq.qos'])){
                    $size=isset($this->config['rabbitmq.qos.size'])?$this->config['rabbitmq.qos.size']:null;
                    $count=isset($this->config['rabbitmq.qos.count'])?$this->config['rabbitmq.qos.count']:1;
                    $global=isset($this->config['rabbitmq.qos.global'])?$this->config['rabbitmq.qos.global']:false;
                    $this->RMQ_channel->basic_qos($size, $count, $global);
                }
            }
            foreach($this->config['rabbitmq.queues'] as $queue){
                $name = $queue['name'];
                $exclusive = isset($queue['exclusive']) ? $queue['exclusive'] : false;
                $no_local = isset($queue['no_local']) ? $queue['no_local'] : false;
                $nowait = isset($queue['nowait']) ? $queue['nowait'] : true;
                $noack  = isset($queue['no_ack'])?$queue['no_ack']:false;
                $arguments = isset($queue['arguments']) ? $queue['arguments'] : array();
                $ticket = isset($queue['ticket']) ? $queue['ticket'] : null;
                $consumerTag = isset($queue['consumerTag']) ? $queue['consumerTag'] : false;
                $arguments=array_merge($arguments,['x-cancel-on-ha-failover' => ['t', true]]);
                $this->RMQ_channel->basic_consume(
                    $name,
                    $consumerTag,
                    $no_local,
                    $noack,
                    $exclusive,
                    $nowait,
                    [$this, 'messageHandler'],
                    $ticket,
                    $arguments
                );
            }
            foreach($temp_queues as $queue){
                $name = $queue['name'];
                $exclusive = isset($queue['exclusive']) ? $queue['exclusive'] : false;
                $no_local = isset($queue['no_local']) ? $queue['no_local'] : false;
                $nowait = isset($queue['nowait']) ? $queue['nowait'] : true;
                $noack  = isset($queue['no_ack'])?$queue['no_ack']:false;
                $arguments = isset($queue['arguments']) ? $queue['arguments'] : array();
                $ticket = isset($queue['ticket']) ? $queue['ticket'] : null;
                $consumerTag = isset($queue['consumerTag']) ? $queue['consumerTag'] : false;
                $arguments=array_merge($arguments,['x-cancel-on-ha-failover' => ['t', true]]);
                $this->RMQ_channel->basic_consume(
                    $name,
                    $consumerTag,
                    $no_local,
                    $noack,
                    $exclusive,
                    $nowait,
                    [$this, 'messageHandler'],
                    $ticket,
                    $arguments
                );
            }
            while (count($this->RMQ_channel->callbacks) && !$this->stop_signal) {
                $this->RMQ_channel->wait(null,true);
            }
            if($this->stop_signal){
                $this->stop();
            }
        }

        public function sendResponse($message){
            $propieties=$this->message->get_properties();
            if(array_key_exists('correlation_id',$propieties) && array_key_exists('reply_to',$propieties)) {
                if (is_array($message))
                    $message = json_encode($message);

                $msg = new AMQPMessage($message, array('correlation_id' => $this->message->get('correlation_id')));
                $this->message->delivery_info['channel']->basic_publish($msg, '', $this->message->get('reply_to'));
            }
        }
        /**
         * Message handler
         *
         * @param  AMQPMessage $message
         * @return void
         */
        public function messageHandler(AMQPMessage $message)
        {
            $this->shm->write($this->id,true);
            $start_time=microtime(true);
            $this->message=$message;
            $query=json_decode($message->body,true);
            $message->delivery_info['channel']->basic_ack($message->delivery_info['delivery_tag']);
            $class=$this->worker_class;
            $action=new $class($this->getLogger(),$this->getConfig(),$query,[$this,'sendResponse']);
            if($action->finished()){
                $end_time=microtime(true);
                $this->sendResponse(json_encode(['response'=>'OK','time'=>($end_time-$start_time)]));
            }
            if ($message->body === 'quit') {
                $message->delivery_info['channel']->basic_cancel($message->delivery_info['consumer_tag']);
            }
            $this->shm->write($this->id,false);
        }



        /**
         * Signal handler
         *
         * @param  int $signalNumber
         * @return void
         */
        public function signalHandler($signalNumber)
        {
            switch ($signalNumber) {
                case SIGTERM:  // 15 : supervisor default stop
                case SIGQUIT:  // 3  : kill -s QUIT
                case SIGUSR1:
                    $this->logger->addNotice("Recieved signal #".Server::$SIGNALS[$signalNumber].'! Stopping gracefully',array('worker'=>$this->id));
                    $this->stop_signal=true;
                    exit(0);
                    break;
                case SIGINT:   // 2  : ctrl+c
                    $this->logger->addNotice("Recieved signal #".Server::$SIGNALS[$signalNumber].'! Stopping gracefully',array('worker'=>$this->id));
                    exit(0);
                    break;
                case SIGHUP:   // 1  : kill -s HUP
                    $this->logger->addNotice("Recieved signal #".Server::$SIGNALS[$signalNumber].'! Sending Memory Usage',array('worker'=>$this->id));
                    pcntl_alarm(1);
                    break;
                case SIGUSR2:  // 12 : kill -s USR2
                    // send an alarm in 10 seconds
                    pcntl_alarm(10);
                    break;
                default:
                    break;
            }
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
        /**
         * Alarm handler
         *
         * @param  int $signalNumber
         * @return void
         */
        public function alarmHandler($signalNumber)
        {
            $this->logger->addDebug("Recieved alarm #".Server::$SIGNALS[$signalNumber].'!');
            $this->logger->addNotice('Memory usage: '.memory_get_usage(true),array('worker'=>$this->id));
            return;
        }



    }