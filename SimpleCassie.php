<?php
$thrift_root = '/usr/share/php/Thrift';
require_once $thrift_root . '/Thrift.php';
require_once $thrift_root . '/protocol/TBinaryProtocol.php';
require_once $thrift_root . '/transport/TSocket.php';
require_once $thrift_root . '/transport/TBufferedTransport.php';
require_once $thrift_root . '/transport/TFramedTransport.php';
require_once $thrift_root . '/packages/cassandra/Cassandra.php';

/*
 * SimpleTools Framework.
 * Copyrights (c) 2010, Marcin Rosinski. (http://www.33concept.com)
 * All rights reserved.
 * 
 * LICENCE
 *
 * Redistribution and use in source and binary forms, with or without modification, 
 * are permitted provided that the following conditions are met:
 *
 * - 	Redistributions of source code must retain the above copyright notice, 
 * 		this list of conditions and the following disclaimer.
 * 
 * -	Redistributions in binary form must reproduce the above copyright notice, 
 * 		this list of conditions and the following disclaimer in the documentation and/or other 
 * 		materials provided with the distribution.
 * 
 * -	Neither the name of the Simpletags.org nor the names of its contributors may be used to 
 * 		endorse or promote products derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR 
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY 
 * AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR 
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL 
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, 
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER 
 * IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF 
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 * 
 * @framework		SimpleTools
 * @packages    	SimpleCassie Client & Thrift Libraries Licenced to Apache Software Foundation
 * @description		Apache Cassandra Self Contain Client
 * @copyright  		Copyrights (c) 2010 Marcin Rosinski - 33Concept Ltd. (http://www.33concept.com)
 * @contributes		Workdigital Ltd. (www.workdigital.co.uk)
 * @license    		http://www.opensource.org/licenses/bsd-license.php - BSD
 * @version    		Ver: 0.7.1.6 2010-12-06 17:50
 * @fork          https://github.com/giltotherescue/simplecassie
 * @forkversion   1.0
 *
 *  CHANGELOG:
 *
 *  2012-04-12    Forked version 1.0
 *                Gil Hildebrand <gilhildebrand@gmail.com>
 *
 *                -Add parse() method for more sane
 *                -TTL support by Zhengjun Chen <zhjchen.sa@gmail.com>
 *                -Remove Thrift client dependencies from this file (the compiled version offers much better performance)
 *                -Change the default number of rows in count() from 100 to 100,000
 *                -Changed the order of arguments on the count() function for backwards compatibility
 */

class SimpleCassie {
  private $__connection = null;
  private $__client = null;

  private $__keyspace = null;
  private $__columnFamily = null;
  private $__key = null;
  private $__column = null;
  private $__superColumn = null;

  private $__pcolumnFamily = null;
  private $__pkey = null;
  private $__pcolumn = null;
  private $__psuperColumn = null;

  private $__connected = false;

  private $__connectTries = 0;

  private $__nodes = array();
  private $__activeNode = null;

  private $__i64time = false;

  private $__batchContainer = array();
  private $__batchSize = 0;

  public function useI64Timestamps() {
    $this->__i64time = true;
  }

  public function uuid($uuid = null) {
    return new SimpleCassieUuid($uuid);
  }

  public function time() {
    if (!$this->__i64time)
      return time(); else {
      $time = explode(' ', microtime());
      $mili = round($time[0] * 1000);
      $secs = $time[1] * 1000;

      return $secs + $mili;
    }
  }

  public function isConnected() {
    if ($this->__connectTries == 0)
      $this->__connect();
    return (boolean)$this->__connected;
  }

  public function getActiveNode() {
    if ($this->__connectTries == 0)
      $this->__connect();
    return $this->__activeNode;
  }

  private function __resetPath() {
    $this->__pcolumnFamily = $this->__columnFamily;
    $this->__pkey = $this->__key;
    $this->__pcolumn = $this->__column;
    $this->__psuperColumn = $this->__superColumn;

    $this->__columnFamily = null;
    $this->__key = null;
    $this->__column = null;
    $this->__superColumn = null;
  }

  public function restorePath() {
    $this->cf($this->__pcolumnFamily)->key($this->__pkey)->supercolumn($this->__psuperColumn)->column($this->__pcolumn);
  }

  private function __connect() {
    foreach ($this->__nodes as $host => $settings) {
      $this->__connectTries++;

      $port = $settings[0];
      $timeout = $settings[1];

      try {
        $socket = new TSocket($host, $port);

        if ($timeout && $timeout > 0) {
          $socket->setSendTimeout($timeout);
          $socket->setRecvTimeout($timeout);
        }

        $connection = new TFramedTransport($socket, 1024, 1024);
        $this->__connectiton = $connection;
        $this->__connectiton->open();
        $this->__connected = $this->__connectiton->isOpen();
      } catch (Exception $e) {
        $this->__connected = false;
      }

      if (!function_exists('thrift_protocol_read_binary'))
        $this->__client = new CassandraClient(new TBinaryProtocol($this->__connectiton)); else
        $this->__client = new CassandraClient(new TBinaryProtocolAccelerated($this->__connectiton));


      if ($this->__connected) {
        $node = new stdClass();
        $node->host = $host;
        $node->port = $port;
        $node->timeout = $timeout;

        $this->__activeNode = $node;
        break;
      }
    }
  }

  public function __construct($host, $port = 9160, $timeout = null) {
    $this->__nodes[$host] = array($port, $timeout);
  }

  public function addNode($host, $port = 9160, $timeout = null) {
    $this->__nodes[$host] = array($port, $timeout);
  }

  public function __destruct() {
    if ($this->__connected)
      $this->__connectiton->close();
  }

  public function &getClient() {
    if (!$this->__connected)
      return false;
    return $this->__client;
  }

  /*
      * DEPRECATED - USE remove() instead
      */
  public function delete($consistencyLevel = cassandra_ConsistencyLevel::ALL) {
    return $this->remove($consistencyLevel);
  }

  public function remove($consistencyLevel = cassandra_ConsistencyLevel::ALL) {
    if (!$this->__connected)
      return -1;

    if (!is_array($this->__key) && !is_array($this->__column)) {
      try {
        $this->__client->remove($this->__key, $this->__getColumn(), $this->time(), $consistencyLevel);
        $this->__resetPath();
        return true;
      } catch (Exception $e) {
        $this->__resetPath();
        return false;
      }
    } else {
      $this->__resetPath();
      return false;
    }
  }

  public function truncate() {
    try {
      $this->__client->truncate($this->__columnFamily);
      $this->__resetPath();
      return true;
    } catch (Exception $e) {
      $this->__resetPath();
      return false;
    }
  }

  public function count($consistencyLevel = cassandra_ConsistencyLevel::ONE, $count = 1000000, $reversed = false) {
    if (!$this->__connected)
      return null;
    if (!is_array($this->__column)) {
      $start = '';
      $finish = '';
    } else {
      $start = $this->__column[0];
      $finish = $this->__column[1];
    }

    $slicePredicate = new cassandra_SlicePredicate(array('slice_range' => new cassandra_SliceRange(array('start' => $start, 'finish' => $finish, 'reversed' => $reversed, 'count' => $count))));

    if (!is_array($this->__key)) {
      try {
        $c = $this->__client->get_count($this->__key, $this->__getColumnParent(), $slicePredicate, $consistencyLevel);
        $this->__resetPath();
        return $c;

      } catch (Exception $e) {
        $this->__resetPath();
        return false;
      }
    } else {
      try {
        $c = $this->__client->multiget_count($this->__key, $this->__getColumnParent(), $slicePredicate, $consistencyLevel);
        $this->__resetPath();
        return $c;
      } catch (Exception $e) {
        $this->__resetPath();
        return false;
      }
    }
  }

  public function get($consistencyLevel = cassandra_ConsistencyLevel::ONE) {
    if (!$this->__connected)
      return null;

    if (!is_array($this->__key)) {
      if (!is_array($this->__column)) {
        try {
          $o = $this->__client->get($this->__key, $this->__getColumn(), $consistencyLevel);
          $this->__resetPath();
          return $o;
        } catch (Exception $e) {
          $this->__resetPath();
          if ($e instanceof cassandra_NotFoundException)
            return null;
        }
        ;
      } else {
        try {
          $o = $this->__client->get_slice($this->__key, $this->__getColumnParent(), new cassandra_SlicePredicate(array('column_names' => $this->__column)), $consistencyLevel);
          $this->__resetPath();
          return $o;
        } catch (Exception $e) {
          $this->__resetPath();
          if ($e instanceof cassandra_NotFoundException)
            return null;
        }
        ;
      }
    } else {
      if (!is_array($this->__column))
        $this->__column = array($this->__column);

      try {
        $o = $this->__client->multiget_slice($this->__key, $this->__getColumnParent(), new cassandra_SlicePredicate(array('column_names' => $this->__column)), $consistencyLevel);
        $this->__resetPath();
        return $o;
      } catch (Exception $e) {
        $this->__resetPath();
        if ($e instanceof cassandra_NotFoundException)
          return null;
      }
      ;
    }
  }

  public function range($keyCount = 100, $columnCount = false, $reversed = false, $consistencyLevel = cassandra_ConsistencyLevel::ONE) {
    if (!$this->__connected)
      return null;

    if ($columnCount OR !$this->__column) {
      if (!$columnCount)
        $columnCount = 100;

      if (!is_array($this->__column)) {
        if ($this->__column)
          $start = $this->__column; else
          $start = '';

        $finish = '';
      } else {
        $start = $this->__column[0];
        $finish = $this->__column[1];
      }

      $predicate = new cassandra_SlicePredicate(array('slice_range' => new cassandra_SliceRange(array('start' => $start, 'finish' => $finish, 'reversed' => $reversed, 'count' => $columnCount))));
    } else
      $predicate = new cassandra_SlicePredicate(array('column_names' => $this->__column));

    if (is_array($this->__key)) {
      $key_range = new cassandra_KeyRange(array('start_key' => $this->__key[0], 'end_key' => $this->__key[1], 'count' => $keyCount));
    } else {
      $key_range = new cassandra_KeyRange(array('start_key' => $this->__key, 'end_key' => '', 'count' => $keyCount));
    }

    try {
      $o = $this->__client->get_range_slices($this->__getColumnParent(), $predicate, $key_range, $consistencyLevel);
      $this->__resetPath();
      return $o;
      /*
           $r = new stdClass();
           foreach($res as $v)
           {
             $r->{$v->key} = new stdClass();

             foreach($v->columns as $c)
             {
               print_r($c);
               if(!$c->column->supercolumn)
                 $r->{$v->key}->{$c->column->name} = $c->column->value;
               else
               {
                 if(!isset($r->{$v->key}->{$c->column->supercolumn})) $r->{$v->key}->{$c->column->supercolumn} = new stdClass();
                 $r->{$v->key}->{$c->column->supercolumn}->{$c->column->name} = $c->column->value;
               }
             }
           }

           return $r;
           */
    } catch (Exception $e) {
      $this->__resetPath();
      return false;
    }
  }

  public function slice($count = 100, $reversed = false, $consistencyLevel = cassandra_ConsistencyLevel::ONE) {
    if (!$this->__connected)
      return null;

    if (!is_array($this->__column)) {
      if (is_array($this->__superColumn)) {
        $start = $this->__superColumn[0];
        $finish = $this->__superColumn[1];
      } else {
        $start = '';
        $finish = '';
      }
    } else {
      $start = $this->__column[0];
      $finish = $this->__column[1];
    }

    $slicePredicate = new cassandra_SlicePredicate(array('slice_range' => new cassandra_SliceRange(array('start' => $start, 'finish' => $finish, 'reversed' => $reversed, 'count' => $count))));

    if (!is_array($this->__key)) {
      try {
        $o = $this->__client->get_slice($this->__key, $this->__getColumnParent(), $slicePredicate, $consistencyLevel);
        $this->__resetPath();
        return $o;
      } catch (Exception $e) {
        $this->__resetPath();
        return false;
      }
    } else {
      try {
        $o = $this->__client->multiget_slice($this->__key, $this->__getColumnParent(), $slicePredicate, $consistencyLevel);
        $this->__resetPath();
        return $o;
      } catch (Exception $e) {
        $this->__resetPath();
        return false;
      }
    }
  }

  public function value($consistencyLevel = cassandra_ConsistencyLevel::ONE) {
    if (!$this->__connected)
      return null;
    $res = $this->get($consistencyLevel);

    if (!is_array($res))
      return isset($res->column->value) ? $res->column->value : null; else {
      $cols = new stdClass();
      foreach ($res as $k => $r) {
        if (!is_array($r))
          $cols->{$r->column->name} = $r->column->value; else {
          foreach ($r as $_r) {
            $cols->{$k}->{$_r->column->name} = $_r->column->value;
          }
        }
      }

      return $cols;
    }
  }

  public function timestamp($consistencyLevel = cassandra_ConsistencyLevel::ONE) {
    if (!$this->__connected)
      return null;
    $res = $this->get($consistencyLevel);

    if (!is_array($res))
      return isset($res->column->timestamp) ? $res->column->timestamp : null; else {
      $cols = new stdClass();
      foreach ($res as $k => $r) {
        if (!is_array($r))
          $cols->{$r->column->name} = $r->column->timestamp; else {
          foreach ($r as $_r) {
            $cols->{$k}->{$_r->column->name} = $_r->column->timestamp;
          }
        }
      }

      return $cols;
    }
  }

  private function __collectBatch($val = null, $ttl = null) {
    $batch = array('cf' => $this->__columnFamily, 'key' => $this->__key, 'supercolumn' => $this->__superColumn, 'column' => $this->__column,);

    if ($val !== null) {
      $batch['value'] = $val;
      $batch['ttl'] = $ttl;
    }
    $this->__batchContainer[] = $batch;
    $this->__batchSize++;
  }

  public function batchCommit($consistencyLevel = cassandra_ConsistencyLevel::ALL) {
    if ($this->__batchSize == 0)
      return 0;

    $mutationMap = array();
    foreach ($this->__batchContainer as $b) {
      if (isset($b['value'])) {
        $mutation = new cassandra_mutation(array('column_or_supercolumn' => new cassandra_ColumnOrSuperColumn(array('column' => ($b['supercolumn'] == '') ? new cassandra_Column(array('name' => $b['column'], 'value' => $b['value'], 'timestamp' => $this->time(), 'ttl' => $b['ttl'])) : null,

          'super_column' => ($b['supercolumn'] != '') ? new cassandra_SuperColumn(array('name' => $b['supercolumn'], 'columns' => array(new cassandra_Column(array('name' => $b['column'], 'value' => $b['value'], 'timestamp' => $this->time(), 'ttl' => $b['ttl']))))) : null))));
      } else {
        $mutation = new cassandra_mutation(array('deletion' => new cassandra_Deletion(array('timestamp' => $this->time(), 'super_column' => $b['supercolumn'], 'predicate' => ($b['column'] != '') ? new cassandra_SlicePredicate(array('column_names' => array($b['column']))) : null))));
      }

      $mutationMap[$b['key']][$b['cf']][] = $mutation;
      $mutation = null;
    }

    $this->__batchContainer = array();
    $size = $this->__batchSize;
    $this->__batchSize = 0;

    try {
      $this->__client->batch_mutate($mutationMap, $consistencyLevel);
      return $size;
    } catch (Exception $e) {
      return false;
    }
  }

  public function batch($value = null, $ttl = null) {
    $this->__collectBatch($value, $ttl);
    $this->__resetPath();
    return $this->__batchSize;
  }

  public function set($value, $consistencyLevel = cassandra_ConsistencyLevel::ALL, $ttl = null) {
    if (!$this->__connected)
      return false;

    try {
      $col = new cassandra_Column();
      $col->name = $this->__column;
      $col->timestamp = $this->time();
      $col->value = $value;
      $col->ttl = $ttl;
      $this->__client->insert($this->__key, $this->__getColumn(), $col, $consistencyLevel);
      $this->__resetPath();
      return true;
    } catch (Exception $e) {
      $this->__resetPath();
      return false;
    }
  }

  public function increment($step = 1, $consistencyLevel = cassandra_ConsistencyLevel::ONE) {
    if (!$this->__connected)
      return false;

    $val = (integer)$this->value($consistencyLevel);

    $this->restorePath();
    $this->set($val + $step, $consistencyLevel);

    return $val + $step;
  }

  public function decrement($step = 1, $consistencyLevel = cassandra_ConsistencyLevel::ONE) {
    if (!$this->__connected)
      return false;

    $val = (integer)$this->value($consistencyLevel);

    $this->restorePath();
    $this->set($val - $step, $consistencyLevel);

    return $val - $step;
  }

  private function __getColumnParent() {
    return new cassandra_ColumnParent(array('column_family' => $this->__columnFamily, 'super_column' => is_array($this->__superColumn) ? null : $this->__superColumn));
  }

  private function __getColumn() {
    return new cassandra_ColumnPath(array('column_family' => $this->__columnFamily, 'column' => $this->__column, 'super_column' => $this->__superColumn));
  }

  public function &keyspace($keyspace = null) {
    if ($this->__connectTries == 0)
      $this->__connect();

    if ($keyspace === null)
      return $this->__keyspace;
    if ($this->__keyspace == $keyspace)
      return $this;

    $this->__keyspace = $keyspace;

    try {
      $r = $this->__client->set_keyspace($keyspace);
    } catch (Exception $e) {
    }

    return $this;
  }

  public function &cf($cf) {
    $this->__columnFamily = $cf;
    return $this;
  }

  public function &key($key) {
    $numargs = func_num_args();
    if ($numargs > 1)
      $this->__key = func_get_args(); else
      $this->__key = $key;
    return $this;
  }

  public function &column($column) {
    $numargs = func_num_args();
    if ($numargs > 1)
      $this->__column = $this->__parseColArgs(func_get_args()); else
      $this->__column = ($column instanceof SimpleCassieUuid) ? $column->uuid : $column;

    return $this;
  }

  private function __parseColArgs($args) {
    $newargs = array();

    foreach ($args as $a) {
      $newargs[] = ($a instanceof SimpleCassieUuid) ? $a->uuid : $a;
    }

    return $newargs;
  }

  public function &supercolumn($supercolumn) {
    $numargs = func_num_args();
    if ($numargs > 1)
      $this->__superColumn = $this->__parseColArgs(func_get_args()); else
      $this->__superColumn = ($supercolumn instanceof SimpleCassieUuid) ? $supercolumn->uuid : $supercolumn;

    return $this;
  }

  function parse($response, $master_timestamp = true) {
    $return = array();

    if (is_array($response) && count($response) > 0) {

      foreach ($response as $key => $row) {
        if (is_object($row)) {
          // probably getting slice of a column (may or not be within a supercolumn)
          if (is_object($row->column) && count($row->column) > 0) {
            $return[$row->column->name] = array('value' => $row->column->value, 'timestamp' => $row->column->timestamp);
          } else if (is_object($row->super_column) && count($row->super_column) > 0) {
            $return[$row->super_column->name] = array();
            foreach ($row->super_column->columns as $sub_row) {
              if ($master_timestamp) {
                $return[$row->super_column->name][$sub_row->name] = $sub_row->value;
                $return[$row->super_column->name]['__timestamp'] = $sub_row->timestamp;
              } else {
                $return[$row->super_column->name][$sub_row->name] = array('value' => $sub_row->value, 'timestamp' => $sub_row->timestamp);
              }
            }
          }
        } else if (is_array($row)) {
          // probably getting supercolumn slice for multiple keys at once
          foreach ($row as $key2 => $row2) {
            if (is_object($row2->column) && count($row2->column) > 0) {
              $return[$key][$row2->column->name] = array('value' => $row2->column->value, 'timestamp' => $row2->column->timestamp);
            } else if (is_object($row2->super_column) && count($row2->super_column) > 0) {
              $return[$key][$row2->super_column->name] = array();
              foreach ($row2->super_column->columns as $sub_row) {
                if ($master_timestamp) {
                  $return[$key][$row2->super_column->name][$sub_row->name] = $sub_row->value;
                  $return[$key][$row2->super_column->name]['__timestamp'] = $sub_row->timestamp;
                } else {
                  $return[$key][$row2->super_column->name][$sub_row->name] = array('value' => $sub_row->value, 'timestamp' => $sub_row->timestamp);
                }
              }
            }
          }
        }
      }
    } else if (is_object($response)) {
      // probably the result of get() on a single column
      $row = $response;
      if (is_object($row->column) && count($row->column) > 0) {
        $return = array('value' => $row->column->value, 'timestamp' => $row->column->timestamp);
      } else if (is_object($row->super_column) && count($row->super_column) > 0) {
        foreach ($row->super_column->columns as $sub_row) {
          if ($master_timestamp) {
            $return[$sub_row->name] = $sub_row->value;
            $return['__timestamp'] = $sub_row->timestamp;
          } else {
            $return[$sub_row->name] = array('value' => $sub_row->value, 'timestamp' => $sub_row->timestamp);
          }
        }
      }
    }

    return $return;
  }

}

class SimpleCassieUuid {
  const interval = 0x01b21dd213814000;
  const clearVar = 63; // 00111111  Clears all relevant bits of variant byte with AND
  const varRFC = 128; // 10000000  The RFC 4122 variant (this variant)
  const clearVer = 15; // 00001111  Clears all bits of version byte with AND
  const version1 = 16; // 00010000

  public function __construct($uuid = null) {
    if ($uuid === null)
      $this->uuid = $this->__uuid(); else
      $this->uuid = $uuid;

    $this->uuid_string = $this->__toString();
  }

  private function __randomUuidBytes($bytes) {
    $rand = "";
    for ($a = 0; $a < $bytes; $a++) {
      $rand .= chr(mt_rand(0, 255));
    }
    return $rand;
  }

  public function __toString() {
    $uuid = $this->uuid;
    return bin2hex(substr($uuid, 0, 4)) . "-" . bin2hex(substr($uuid, 4, 2)) . "-" . bin2hex(substr($uuid, 6, 2)) . "-" . bin2hex(substr($uuid, 8, 2)) . "-" . bin2hex(substr($uuid, 10, 6));
  }

  private function __uuid() {
    /*
         * Based on Zend_Uuid - Christoph Kempen & Danny Verkade script
         * Generates a Version 1 UUID.
        These are derived from the time at which they were generated. */
    // Get time since Gregorian calendar reform in 100ns intervals
    // This is exceedingly difficult because of PHP's (and pack()'s)
    //  integer size limits.
    // Note that this will never be more accurate than to the microsecond.
    $time = microtime(1) * 10000000 + self::interval;
    // Convert to a string representation
    $time = sprintf("%F", $time);
    preg_match("/^\d+/", $time, $time); //strip decimal point
    // And now to a 64-bit binary representation
    $time = base_convert($time[0], 10, 16);
    $time = pack("H*", str_pad($time, 16, "0", STR_PAD_LEFT));
    // Reorder bytes to their proper locations in the UUID
    $uuid = $time[4] . $time[5] . $time[6] . $time[7] . $time[2] . $time[3] . $time[0] . $time[1];
    // Generate a random clock sequence
    $uuid .= $this->__randomUuidBytes(2);
    // set variant
    $uuid[8] = chr(ord($uuid[8]) & self::clearVar | self::varRFC);
    // set version
    $uuid[6] = chr(ord($uuid[6]) & self::clearVer | self::version1);
    // Set the final 'node' parameter, a MAC address

    // If no node was provided or if the node was invalid,
    //  generate a random MAC address and set the multicast bit
    $node = $this->__randomUuidBytes(6);
    $node[0] = pack("C", ord($node[0]) | 1);

    $uuid .= $node;
    return $uuid;
  }

  public static function binUuid($uuid) {
    return pack("H*", $uuid);
  }
}

?>
