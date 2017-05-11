/**
 * Created by Li Shangjing  ,  2017-03-29 .
 */
var mysql = require( 'mysql' );
var dbConfig = require( "./../../config/dbConfig" );

var pools = {};
module.exports = Db;
var keywords={
	"key" : true
}

function Db( dbName ) {
	var _this = this;
	if( !( this instanceof Db ) ){
		return new Db( dbName );
	}

	if( !dbName ){
		dbName = "defaultDB";
	}

	this.dbName = dbName;

}
Db.init = function( dbName ){
	return new Db( dbName );
}
Db.prototype.queryFirst = async function( sql , args  , callback   ){
	var obj =  await this.query( sql , args , callback , true );
	return obj;
}
Db.prototype.insert = async function( tableName ,  obj , fields ){
	if( !fields )
		fields = Object.getOwnPropertyNames( obj );
	var fieldlen = fields.length ;
	var sql = "insert into " + tableName +"( "+ fields.join(",") +") values( " + getQuestionMarks( fields.length)  + ") ";
	var args = new Array( fieldlen );
	for( var i=0;i<fieldlen ;i++ ){
		args[ i ] = obj[ fields[i]  ];
	}
	var ret = await this.query( sql , args );
	return ret.insertId;

}

Db.prototype.update = async function( tableName  , pks  ,  obj , fields ){

	if( ! (pks instanceof Array ))
		pks = [ pks ];

	var options = new Array( pks.length );
	var optPra  =  new Array( pks.length );
	var isPkCheck = {};

	for( var i=0 ; i < pks.length ; i++ ){
		var key = pks[ i ];
		options[ i ] =  key+"=?" ; // obj[ key ];
		optPra[  i ] = obj[ key ];
		isPkCheck[ key ] = true ;
	}

	if( !fields )
		fields = Object.getOwnPropertyNames( obj );

	var fieldlen = fields.length ;
	var sql =[ "update " + tableName +" set " ];
	var args = [];
	var updates =[];

	for( var i=0;i<fieldlen ;i++ ){
		var field =  fields[i];
		if( isPkCheck[ field ] )
			continue;
		var val = obj[ field ];
		if( val===null ){
			updates.push(  field + "=null" );
		}else{
			args.push(   obj[ field ] );
			updates.push(  field + "=?" );
		}
	}
	sql.push( updates.join(",") );
	sql.push( " where " );
	sql.push( options.join(" and ") );
	sql = sql.join( "" );

	args.push( optPra );

	var ret = await this.query( sql , args );
	return ret;

}
var questionmarks= "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?";
function getQuestionMarks( len ){
	var strlen = len * 2 -1;
	var qks = questionmarks.substring( 0 , strlen );
	if( qks.length<strlen ){
		questionmarks = questionmarks + "," + questionmarks;
		return getQuestionMarks( len );
	}
	return qks;
}
/**
 *
 * 查询数据库
 *
 * 注：如果有callback , 出错时，会调用callback ,同时也会执行 reject方法 ， 保证流程不会断
 *      实际调用，不应当同时使用传入callback的方式 和使用await的方式。
 */
Db.prototype.query = async function( sql , args  , callback , firstOnly ){
	var pool    = getPool(  this.dbName ) ;
	if( callback &&  typeof callback !="function"){
		throw new Error("参数3 callback 不是 function");
	}
	return await new Promise(function( resolve , reject ){
		pool.getConnection(function( err , conn ){
			if( err ){
				if( callback )
					callback( err  );
				console.info( err );
				return reject( err );
			}
			try{

				conn.query( sql , args , function( err , result ){
					if( firstOnly === true ){
						result = result && result[ 0 ];
					}
					if( err){
						console.error( sql , args )
						err.sql = sql ;
						err.params = args;
					}
					if( callback )
						callback( err , result );
					if( err ) {
						if( err.cocde="ER_PARSE_ERROR") {
							var newerr = new Error( err.message );
							newerr.sql = err.sql;
							newerr.sqlState = err.sqlState;
							newerr.errno    = err.errno;
							newerr.code     = err.code;
							newerr.params   = err.params;
							reject( err );
						}else
							reject(err);
					}else
						resolve( result );
				});
			}catch( e ){
				console.error( sql );
				if( callback )
					callback( e  );
				reject( e );
			}finally{
				conn.release();
			}
		});
	});

}
Db.prototype.queryPage = async function( sql , pageSize , pageIndex , params   ){
	var limit = " limit " + ( (pageIndex-1) * pageSize  ) +"," +  pageSize;
	var rs = await this.query( sql + limit  , params );
	return rs;
}
Db.prototype.queryPageAndCount = async function( fields , fromWhere , pageSize ,  pageIndex , params   ){
	var limit = " limit " + ( (pageIndex-1) * pageSize  ) +"," +  pageSize;
	if(!/^\s*from/.test(fromWhere))
		fromWhere = " from " + fromWhere;
	var sql = "select " + fields + " "  +  fromWhere + limit ;
	var sqlCount = "select count(*) cnt " +  fromWhere  ;
	var ret = {};
	ret.recordsTotal = ( await this.queryFirst( sqlCount , params ) ).cnt;
	ret.list   = await this.query( sql , params );
	ret.recordsFiltered  = ret.recordsTotal;
	return ret;
}

/**
 * 在一个连接中执行多个操作，可以开启事务
 */
Db.prototype.exec = async function( runner ){
	var pool    = getPool(  this.dbName ) ;
	return new Promise( function( resolve , reject ){
		pool.getConnection( async function( err , conn ){
			if( err ){
				console.info( err );
				return reject( err );
			}
			var db = new TransDb(  );
			db.conn = conn;
			try{
				await runner( db );
				resolve();
			}catch( ex ){
				try{ db.rollback(); } catch( ignore ){}
				reject( ex );
			}finally{
				db.conn = null;
				conn.release();
				conn = null;

			}

		});
	});
}
function TransDb ( conn ){
	var _this= this;
	this.conn = conn;
	this.queryPage = Db.prototype.queryPage ;
	this.queryPageAndCount = Db.prototype.queryPageAndCount ;
	this.query = async function query(  sql , args){
		var conn = _this.conn;
		return new Promise(function( resolve , reject ){
			try{
				if( conn==null ){
					reject( new Error("connection is null") );
				}
				conn.query( sql , args , function( err , result ){
					if( err )
						reject( err );
					else
						resolve( result );
				});
			}catch( e ){
				reject( e );
			}finally{
				//这里不要关闭数据库连接
			}
		});
	}

	this.beginTransaction = async function beginTransaction(  ){
		return this.query("start TRANSACTION");
		// if( conn ){
		// 	return new Promise(function( resolve , reject ){
		// 		conn.beginTransaction( function( err ){
		// 			if( err ){
		// 				return reject();
		// 			}
		// 			resolve();
		// 		} );
		// 	});
		// }
	}

	this.commit = async  function commit(  ){
		return this.query("commit");
		// if( conn ){
		// 	return new Promise(function( resolve , reject ){
		// 		conn.commit( function( err ){
		// 			if( err ){
		// 				return reject();
		// 			}
		// 			resolve();
		// 		} );
		// 	});
		// }
	}
	this.rollback = async  function rollback(  ){
		return this.query("rollback");
		// if( conn ){
		// 	return new Promise(function( resolve , reject ){
		// 		conn.rollback( function( err ){
		// 			if( err ){
		// 				return reject();
		// 			}
		// 			resolve();
		// 		} );
		// 	});
		// }
	}
}

function getPool( dbName ){

	var pool    = pools[ dbName ] ;
	if( !pool  ){
		var config = dbConfig.config[ dbName ];
		if( config==null){
			throw new Error( dbName+"未配置");
		}
		pool = mysql.createPool( config );
		pools[ dbName ] = pool ;
	}
	return pool;
}
