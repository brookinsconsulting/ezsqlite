<?php

class eZSQLiteDB extends eZDBInterface
{
    function __construct( $parameters )
    {
        $this->eZDBInterface( $parameters );

        if ( !extension_loaded( 'sqlite' ) )
        {
            if ( function_exists( 'eZAppendWarningItem' ) )
            {
                eZAppendWarningItem( array( 'error' => array( 'type' => 'ezdb',
                                                              'number' => eZDBInterface::ERROR_MISSING_EXTENSION ),
                                            'text' => 'PDO SQLite extension was not found, the DB handler will not be initialized.' ) );
                $this->IsConnected = false;
            }
            eZDebug::writeError( 'PDO SQLite extension was not found, the DB handler will not be initialized.', 'eZSQLiteDB' );
            return;
        }

        if ( $this->DBConnection == false )
        {
            $this->DBConnection = $this->connect( $this->DB );
        }

        // Initialize TempTableList
        $this->TempTableList = array();

        eZDebug::createAccumulatorGroup( 'sqlite_total', 'SQLite Total' );
    }

    /*!
     \private
     Opens a new connection to a MySQL database and returns the connection
    */
    private function connect( $fileName )
    {
        $connection = false;
        $error = null;

        $maxAttempts = $this->connectRetryCount() + 1;
        $waitTime = $this->connectRetryWaitTime();
        $numAttempts = 1;
        while ( ( $connection == false || $error !== null ) && $numAttempts <= $maxAttempts )
        {
            $fullPath = eZDir::path( array( 'var/storage/sqlite', $fileName ) );
            $connection = new SQLiteDatabase( $fullPath, 0666, $error );
            $numAttempts++;
        }

        if ( $error !== null )
        {
            $this->ErrorNumber = sqlite_error_string( $connection->lastError() );
            $this->ErrorMessage = $error;
            eZDebug::writeError( "Connection error: Couldn't connect to database. Please try again later or inform the system administrator.\n$errorMessage", "eZSQLiteDB" );
            $this->IsConnected = false;
        }
        else
        {
            $connection->createFunction( 'md5', array( $this, 'md5UDF' ) );
            $this->IsConnected = true;
        }

        return $connection;
    }

    /*!
     \reimp
    */
    function databaseName()
    {
        return 'sqlite';
    }

    /*!
      \reimp
    */
    function bindingType( )
    {
        return eZDBInterface::BINDING_NO;
    }

    /*!
      \reimp
    */
    function bindVariable( $value, $fieldDef = false )
    {
        return $value;
    }

    /*
    */
    function checkCharset( $charset, &$currentCharset )
    {
        return true;
    }

    /*!
     \reimp
    */
    function query( $sql, $server = false )
    {
        /*
        print( $sql . PHP_EOL . PHP_EOL );

        $backtrace = debug_backtrace();
        $cleanedBackTrace = array();
        foreach ( $backtrace as $call )
        {
            $item = '';
            if ( isset( $call['class'] ) )
            {
                $item .= $call['class'];
            }

            if ( isset( $call['type'] ) )
            {
                $item .= $call['type'];
            }

            $item .= $call['function'] . " in file " . $call['file'] . " line " . $call['line'];

            //$item .= var_export( $call['args'], true );
            $cleanedBacktrace[] = $item;
        }

        print( implode( PHP_EOL, $cleanedBacktrace ) . PHP_EOL . PHP_EOL );
        */

        $error = false;
        if ( $this->IsConnected )
        {
            if ( $this->OutputSQL )
            {
                eZDebug::accumulatorStart( 'sqlite_query', 'sqlite_total', 'SQLite_queries' );
                $this->startTimer();
            }

            $result = $this->DBConnection->queryExec( $sql, $error );
            if ( $this->OutputSQL )
            {
                $this->endTimer();

                if ( $this->timeTaken() > $this->SlowSQLTimeout )
                {
                    eZDebug::accumulatorStop( 'sqlite_query' );
                    $this->reportQuery( 'SQLiteDB', $sql, false, $this->timeTaken() );
                }
            }

            if ( !$result )
            {
                $backtrace = debug_backtrace();
                $cleanedBackTrace = array();
                foreach ( $backtrace as $call )
                {
                    $item = '';
                    if ( isset( $call['class'] ) )
                    {
                        $item .= $call['class'];
                    }

                    if ( isset( $call['type'] ) )
                    {
                        $item .= $call['type'];
                    }

                    $item .= $call['function'] . " in file " . $call['file'] . " line " . $call['line'];

                    //$item .= var_export( $call['args'], true );
                    $cleanedBacktrace[] = $item;
                }

                eZDebug::writeError( implode( "\r\n", $cleanedBacktrace ) );

                $this->ErrorNumber = $this->DBConnection->lastError();
                $this->ErrorMessage = $error;

                eZDebug::writeError( "Error: error '$error' when executing query: $sql", "eZSQLiteSQLDB" );
                $this->reportError();
            }
            else
            {
                return true;
            }
        }
        else
        {
            eZDebug::writeError( "Trying to do a query without being connected to a database!", "eZSQLiteDB"  );
        }

        return false;
    }

    /*!
     \reimp
    */
    function arrayQuery( $sql, $params = array(), $server = false )
    {
        /*
        print( $sql . PHP_EOL . PHP_EOL );

        $backtrace = debug_backtrace();
        $cleanedBackTrace = array();
        foreach ( $backtrace as $call )
        {
            $item = '';
            if ( isset( $call['class'] ) )
            {
                $item .= $call['class'];
            }

            if ( isset( $call['type'] ) )
            {
                $item .= $call['type'];
            }

            $item .= $call['function'] . " in file " . $call['file'] . " line " . $call['line'];

            //$item .= var_export( $call['args'], true );
            $cleanedBacktrace[] = $item;
        }

        print( implode( PHP_EOL, $cleanedBacktrace ) . PHP_EOL . PHP_EOL );
        */

        $retArray = array();
        if ( $this->IsConnected )
        {
            $limit = false;
            $offset = 0;
            $column = false;
            // check for array parameters
            if ( is_array( $params ) )
            {
                if ( isset( $params["limit"] ) and is_numeric( $params["limit"] ) )
                    $limit = $params["limit"];

                if ( isset( $params["offset"] ) and is_numeric( $params["offset"] ) )
                    $offset = $params["offset"];

                if ( isset( $params["column"] ) and ( is_numeric( $params["column"] ) or is_string( $params["column"] ) ) )
                    $column = $params["column"];
            }

            if ( $limit !== false and is_numeric( $limit ) )
            {
                $sql .= "\nLIMIT $offset, $limit ";
            }
            else if ( $offset !== false and is_numeric( $offset ) and $offset > 0 )
            {
                $sql .= "\nLIMIT $offset, 18446744073709551615"; // 2^64-1
            }

            if ( $this->OutputSQL )
            {
                eZDebug::accumulatorStart( 'sqlite_query', 'sqlite_total', 'SQLite_queries' );
                $this->startTimer();
            }

            $result = $this->DBConnection->arrayQuery( $sql, SQLITE_ASSOC );

            if ( $this->OutputSQL )
            {
                $this->endTimer();

                if ( $this->timeTaken() > $this->SlowSQLTimeout )
                {
                    eZDebug::accumulatorStop( 'sqlite_query' );
                    $this->reportQuery( 'SQLiteDB', $sql, false, $this->timeTaken() );
                }
            }

            if ( $result === false )
            {
                $this->setError();
                eZDebug::writeError( "Error: error executing query: $sql", "eZSQLiteSQLDB" );
                $this->reportError();

                return false;
            }

            $transformedResult = array();

            $numRows = count( $result );

            for ( $i=0; $i < $numRows; $i++ )
            {
                $item = array();
                foreach ( $result[$i] as $identifier => $value )
                {
                    if ( strpos( $identifier, '.' ) !== false )
                    {
                        $parts = explode( '.', $identifier );
                        $newIdentifier = array_pop( $parts );
                    }
                    else
                    {
                        $newIdentifier = $identifier;
                    }

                    $item[$newIdentifier] = $value;
                }

                $transformedResult[$i] = $item;
            }

            if ( $numRows > 0 )
            {
                if ( !is_string( $column ) )
                {
                    eZDebug::accumulatorStart( 'sqlite_loop', 'sqlite_total', 'Looping result' );
                    for ( $i=0; $i < $numRows; $i++ )
                    {
                        $retArray[$i + $offset] = $transformedResult[$i];
                    }
                    eZDebug::accumulatorStop( 'sqlite_loop' );

                }
                else
                {
                    eZDebug::accumulatorStart( 'sqlite_loop', 'sqlite_total', 'Looping result' );
                    for ( $i=0; $i < $numRows; $i++ )
                    {
                        $retArray[$i + $offset] = $transformedResult[$i][$column];
                    }
                    eZDebug::accumulatorStop( 'sqlite_loop' );
                }
            }

            //eZDebug::writeDebug( $retArray );
        }
        return $retArray;
    }

    function subString( $string, $from, $len = null )
    {
        if ( $len == null )
        {
            return " substr( $string, $from, length( $string ) - $from ) ";
        }
        else
        {
            return " substr( $string, $from, $len ) ";
        }
    }

    function concatString( $strings = array() )
    {
        return implode( " || " , $strings );
    }

    function md5( $str )
    {
        return " MD5( $str ) ";
    }

    function md5UDF( $str )
    {
        return md5( $str );
    }

    function bitAnd( $arg1, $arg2 )
    {
        return '(' . $arg1 . ' & ' . $arg2 . ' ) ';
    }

    function bitOr( $arg1, $arg2 )
    {
        return '( ' . $arg1 . ' | ' . $arg2 . ' ) ';
    }

    /*!
     \reimp
     The query to start the transaction.
    */
    function beginQuery()
    {
        return $this->query( "BEGIN" );
    }

    /*!
     \reimp
     The query to commit the transaction.
    */
    function commitQuery()
    {
        return $this->query( "COMMIT" );
    }

    /*!
     \reimp
     The query to cancel the transaction.
    */
    function rollbackQuery()
    {
        return $this->query( "ROLLBACK" );
    }

    /*!
     \reimp
    */
    function lastSerialID( $table = false, $column = false )
    {
        if ( $this->IsConnected )
        {
            $id = $this->DBConnection->lastInsertRowid();

            // if the primary key consists of more than one field
            // then we can not rely on the auto increment functionality of SQLite,
            /// because it only works on a PRIMARY KEY of 1 column
            // so we need to check if the autoincrement field matches the rowid
            // if not, we'll update it
            $result = $this->arrayQuery( "SELECT $column FROM $table WHERE rowid=$id" );
            if ( $result[0][$column] != $id )
            {
                // we use the maximum + 1 instead of the rowid, because some autoincrement fields
                // in the standard data do not follow up each other
                // so for the standard data the autoincrement column might not match the rowid
                // and query errors will appear when we add new data because of unique key violations
                $max = $this->arrayQuery( "SELECT MAX($column) AS maximum FROM $table" );

                $newID = $max['0']['maximum'] + 1;

                $this->query( "UPDATE $table SET $column=$newID WHERE rowid=$id" );

                return $newID;
            }
            else
            {
                return $id;
            }
        }
        else
        {
            return false;
        }
    }

    /*!
     \reimp
    */
    function escapeString( $str )
    {
        return sqlite_escape_string( $str );
    }

    /*!
     \reimp
    */
    function close()
    {
        if ( $this->IsConnected )
        {
            unset( $this->DBConnection );
            $this->IsConnected = false;
        }
    }

    /*!
     \reimp
    */
    function createDatabase( $dbName )
    {
        // useless in the contect of SQLite
    }

    /*!
     \reimp
    */
    function setError()
    {
        if ( $this->DBConnection )
        {
            $this->ErrorNumber = $this->DBConnection->lastError();
            $this->ErrorMessage = sqlite_error_string( $this->ErrorNumber );
        }
    }

    /*!
     \reimp
    */
    function availableDatabases()
    {
        // useless in the contect of SQLite
    }

    /*!
     \reimp
    */
    function databaseServerVersion()
    {
        // no server, so returning client version
        return $this->databaseClientVersion();
    }

    /*!
     \reimp
    */
    function databaseClientVersion()
    {
        $versionInfo = sqlite_libversion();

        $versionArray = explode( '.', $versionInfo );

        return array( 'string' => $versionInfo,
                      'values' => $versionArray );
    }

    /*!
     \reimp
    */
    function isCharsetSupported( $charset )
    {
        return true;
    }

    /*
    */
    function eZTableList( $server = eZDBInterface::SERVER_MASTER )
    {
        $tables = array();
        if ( $this->IsConnected )
        {
            $sql = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name";
            $result = $this->DBConnection->arrayQuery( $sql );

            foreach ( $result as $entry )
            {
                $tableName = $entry['name'];
                if ( substr( $tableName, 0, 2 ) == 'ez' )
                {
                    $tables[$tableName] = eZDBInterface::RELATION_TABLE;
                }
            }
        }
        return $tables;
    }

    /*!
     \reimp
    */
    function supportedRelationTypes()
    {
        return array( eZDBInterface::RELATION_TABLE );
    }

    function relationList( $relationType = eZDBInterface::RELATION_TABLE )
    {
        if ( $relationType != eZDBInterface::RELATION_TABLE )
        {
            eZDebug::writeError( "Unsupported relation type '$relationType'", 'eZSQLiteDB::relationList' );
            return false;
        }

        $tables = array_keys( $this->eZTableList() );
        return $tables;
    }

    /*!
      \reimp
    */
    function removeRelation( $relationName, $relationType )
    {
        $relationTypeName = $this->relationName( $relationType );
        if ( !$relationTypeName )
        {
            eZDebug::writeError( "Unknown relation type '$relationType'", 'eZSQLiteDB::removeRelation' );
            return false;
        }

        if ( $this->IsConnected )
        {
            $sql = "DROP $relationTypeName $relationName";
            return $this->query( $sql );
        }
        return false;
    }

    public $TempTableList;
}

?>