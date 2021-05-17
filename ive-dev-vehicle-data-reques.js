const AWS = require('aws-sdk');
AWS.config.update({ region: 'us-west-2' });
const { Pool } = require("pg");


exports.handler = async (event) => {

  const CONNECTION_STRING = process.env.CONNECTION_STRING;

  // DB VARIABLE DECLARATION
  let db_host = process.env.db_host;
  let db_user = process.env.db_user;
  let db_pass = process.env.db_pass;
  let db_name = process.env.db_name;
  let db_port = process.env.db_port;
  let db_schema = process.env.db_schema;

  // RESPONSE BODY CONSTUCTION
  const response = {
    statusCode: 200,
    body: JSON.stringify('Hello from Lambda!'),
  };

  let prodmonth;
  let request_id;
  let business_entity;
  let prod_path;
  let ssn;
  let katashiki;
  let int;
  let ext;
  let spectotal200;
  let len;
  let earliest_lineoffdate;
  let guid;
  let nqc_req_pattern;
  let idline = '11';

  // CONNECTION_STRING INITIALIZATION
  const connectionString = process.env.connection_string || CONNECTION_STRING;
  // POOL INITIALIZATION
  const pool = new Pool({ connectionString: connectionString });



  const sql_req_be = "SELECT request_id, business_entity from apv_ive.vehicle_data_request_info where request_status=$1 order by requested_date ASC";
  const values = ['started'];
  const resp_req_be = await pool.query(sql_req_be, values);
  for (const key of resp_req_be.rows) {
    business_entity = key['business_entity'];
    request_id = key['request_id'];
    break;
  }


  var array_prod_month = [];
  const sql_prod_month = "SELECT distinct(prodmonth) from apv_ive.vehicle_data_request_details where requestid=$1 order by prodmonth ASC ";
  const values_prod_month = [request_id];
  const resp_prod_month = await pool.query(sql_prod_month, values_prod_month);
  console.log(resp_prod_month);
  for (const key of resp_prod_month.rows) {
    array_prod_month.push(key['prodmonth']);
    console.log("prodmonth **" + key['prodmonth']);
  }
  if (array_prod_month.length < 5) {
    len = array_prod_month.length;

  }
  else {
    len = 5;
  }

  //D2 Logic:
  for (var i = 0; i < len; i++) {
    console.log("array_prod_month **" + array_prod_month[i]);
    prodmonth = array_prod_month[i];

    const sql_earliest_lineoffdate = "SELECT lineoffdate from apv_ive.vehicle_data_request_details where prodmonth=$1 and requestid=$2 and business_entity=$3 order by lineoffdate ASC ";
    const values_earliest_lineoffdate = [prodmonth, request_id, business_entity];
    const resp_earliest_lineoffdate = await pool.query(sql_earliest_lineoffdate, values_earliest_lineoffdate);
    console.log(resp_earliest_lineoffdate);
    for (const key of resp_earliest_lineoffdate.rows) {
      earliest_lineoffdate = key['lineoffdate'];
      console.log("earliest_lineoffdate **" + earliest_lineoffdate);
      break;
    }

    const sql = "SELECT distinct(ssn, prodpath, katashiki, int, ext, spectotal200),ssn, prodpath, katashiki, int, ext, spectotal200 from apv_ive.vehicle_data_request_details where prodmonth=$1 and requestid=$2 and business_entity=$3";
    const values = [prodmonth, request_id, business_entity];
    const resp = await pool.query(sql, values);
    for (const key of resp.rows) {
      console.log("ssn **" + key['ssn']);
      console.log("prodpath **" + key['prodpath']);
      console.log("katashiki **" + key['katashiki']);
      ssn = key['ssn'];
      prodpath = key['prodpath'];
      katashiki = key['katashiki'];
      int = key['int'];
      ext = key['ext'];
      spectotal200 = key['spectotal200'];
      //break;
      //}

      const sql_guid = "SELECT ENCODE (nextval(RAW_GUID_SEQ), 'HEX') FROM DUAL";
      const resp_guid = await pool.query(sql_guid);
      for (const key of resp_guid.rows) {
        guid = key['guid'];
        break;
      }
      var guid_len = guid.toString().length;

      switch (guid_len) {
        case 1:
          guid = "000000000" + guid;
          break;

        case 2:
          guid = "00000000" + guid;
          break;

        case 3:
          guid = "0000000" + guid;
          break;

        case 4:
          guid = "000000" + guid;
          break;

        case 5:
          guid = "00000" + guid;
          break;

        case 6:
          guid = "0000" + guid;
          break;

        case 7:
          guid = "000" + guid;
          break;

        case 8:
          guid = "00" + guid;
          break;

        case 9:
          guid = "0" + guid;
          break;

        case 10:
          guid = guid;
          break;

        default:
          guid = "000000000";
          break;
      }

      const sqld2record = "INSERT INTO apv_ive.earliest_lineoff_date_d2record(guid,ssn,prodpath,katashiki,int,ext,spectotal200,earliest_lineoffdate,prodmonth,requestid,business_entity) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)";
      const valuesd2record = [guid, ssn, prodpath, katashiki, int, ext, spectotal200, earliest_lineoffdate, prodmonth, request_id, business_entity];
      const resd2record = await pool.query(sqld2record, valuesd2record);
      const resultStringReqInfo = JSON.stringify(resd2record);
      console.log("Insert into reqInfo " + resultStringReqInfo);

      //To generate DAILY_UNIT (56) values:
      var array_daily_unit = [];
      for (var j = 1; j <= 49; j++) {
        const sql_daily_unit = "SELECT count(0) from apv_ive.vehicle_data_request_details where ssn=$1 AND prodpath=$2 AND katashiki=$3 AND int= $4 AND ext=$5 AND spectotal200=$6 AND prodmonth=$7 AND earliest_lineoffdate=$8";
        const values_daily_unit = [ssn, prodpath, katashiki, int, ext, spectotal200, prodmonth, earliest_lineoffdate];
        const resp_daily_unit = await pool.query(sql_daily_unit, values_daily_unit);
        for (const key of resp_daily_unit.rows) {
          array_daily_unit[j] = call_append_zero(key['count(0)']);//write a function
          break;
        }
        earliest_lineoffdate = earliest_lineoffdate + 1;
      }

      /* function call_append_zero(du)
       {
         var du_len=du.toString().length;
   
         switch (du_len)
         {
            case 1:
             du = "000000"+du;
                break;
    
            case 2:
             du = "00000"+du;
                break;
    
            case 3:
             du = "0000"+du;
                break;
                
            case 4:
             du = "000"+du;
                break;
    
            case 5:
             du = "00"+du;
                break;
    
            case 6:
             du = "0"+du;
                break;
                
             case 7:
               du = du;
                break;
    
            default:
                guid = "000000";
            break;
         }
       }*/

      const sql_nqc_req_pattern = "SELECT nqc_req_pattern FROM apv_ive.file_id_specifications WHERE business_entity=$1";
      const values_nqc_req_pattern = [business_entity];
      const resp_nqc_req_pattern = await pool.query(sql_nqc_req_pattern, values_nqc_req_pattern);
      for (const key of resp_nqc_req_pattern.rows) {
        nqc_req_pattern = key['nqc_req_pattern'];
        break;
      }

      const sql_dailyunit_insert = "INSERT INTO apv_ive.vehicle_data_request_file(nqc_req_pattern,ssn,idline,control_katashiki,interior_col,exterior_col,specification_total200,vehicle_order_condition,free_prod_month,free_ssn,free_idline,free_requestid,free_guid,daily_unit_1,daily_unit_2,daily_unit_3,daily_unit_4,daily_unit_5,daily_unit_6,daily_unit_7,daily_unit_8,daily_unit_9,daily_unit_10,daily_unit_11,daily_unit_12,daily_unit_13,daily_unit_14,daily_unit_15,daily_unit_16,daily_unit_17,daily_unit_18,daily_unit_19,daily_unit_20,daily_unit_21,daily_unit_22,daily_unit_23,daily_unit_24,daily_unit_25,daily_unit_26,daily_unit_27,daily_unit_28,daily_unit_29,daily_unit_30,daily_unit_31,daily_unit_32,daily_unit_33,daily_unit_34,daily_unit_35,daily_unit_36,daily_unit_37,daily_unit_38,daily_unit_39,daily_unit_40,daily_unit_41,daily_unit_42,daily_unit_43,daily_unit_44,daily_unit_45,daily_unit_46,daily_unit_47,daily_unit_48,daily_unit_49,daily_unit_50,daily_unit_51,daily_unit_52,daily_unit_53,daily_unit_54,daily_unit_55,daily_unit_56,business_entity,earliest_lineoffdate) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50,$51,$52,$53,$54,$55,$56,$57,$58,$59,$60,$61,$62,$63,$64,$65,$66,$67,$68,$69,$70,$71)";
      const values_dailyunit_insert = [nqc_req_pattern, ssn, idline, katashiki, int, ext, spectotal200, '1', prodmonth, ssn, idline, request_id, guid, array_daily_unit[1], array_daily_unit[2], array_daily_unit[3], array_daily_unit[4], array_daily_unit[5], array_daily_unit[6], array_daily_unit[7], array_daily_unit[8], array_daily_unit[9], array_daily_unit[10], array_daily_unit[11], array_daily_unit[12], array_daily_unit[13], array_daily_unit[14], array_daily_unit[15], array_daily_unit[16], array_daily_unit[17], array_daily_unit[18], array_daily_unit[19], array_daily_unit[20], array_daily_unit[21], array_daily_unit[22], array_daily_unit[23], array_daily_unit[24], array_daily_unit[25], array_daily_unit[26], array_daily_unit[27], array_daily_unit[28], array_daily_unit[29], array_daily_unit[30], array_daily_unit[31], array_daily_unit[32], array_daily_unit[33], array_daily_unit[34], array_daily_unit[35], array_daily_unit[36], array_daily_unit[37], array_daily_unit[38], array_daily_unit[39], array_daily_unit[40], array_daily_unit[41], array_daily_unit[42], array_daily_unit[43], array_daily_unit[44], array_daily_unit[45], array_daily_unit[46], array_daily_unit[47], array_daily_unit[48], array_daily_unit[49], '0000000', '0000000', '0000000', '0000000', '0000000', '0000000', '0000000', business_entity, earliest_lineoffdate];
      const res_dailyunit_insert = await pool.query(sql_dailyunit_insert, values_dailyunit_insert);
      const resultString_dailyunit_insert = JSON.stringify(res_dailyunit_insert);
      console.log("Insert into reqInfoDetails " + resultString_dailyunit_insert);
    }

  }
  //D2 logic end

  function call_append_zero(du) {
    var du_len = du.toString().length;

    switch (du_len) {
      case 1:
        du = "000000" + du;
        break;

      case 2:
        du = "00000" + du;
        break;

      case 3:
        du = "0000" + du;
        break;

      case 4:
        du = "000" + du;
        break;

      case 5:
        du = "00" + du;
        break;

      case 6:
        du = "0" + du;
        break;

      case 7:
        du = du;
        break;

      default:
        guid = "000000";
        break;
    }
  }



  return response;




};