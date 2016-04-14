<?php
    /**
     * Created by BrutalSYS SL.
     * User: ellio
     * Date: 15/04/2016
     * Time: 0:47
     */

    namespace BSYS;


    interface WorkerInterface
    {
        public function __construct(\Monolog\Logger $logger,\Configula\Config $config,$query,callable $response);

        /**
         * Return if the process has been finished
         * @return boolean
         */
        public function finished();
    }