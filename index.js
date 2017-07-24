var GoogleSpreadsheet = require('google-spreadsheet');
var async = require('async');
var mysql      = require('mysql');
var exec = require('child_process').exec;
var moment = require('moment');

// spreadsheet key is the long id in the sheets URL
var doc = new GoogleSpreadsheet('1xXD_KVLbinCVd-S8edlcSYpjFX84JRzvG5_LrzE8t_c');
var sheet;
var connection, client_connection;
var therapists = new Object();
var invitations = [];

// therapists['ae6c581e-ba59-44e6-944d-1bd1cb302ac9'] = {
//     'first_name': 'demo_dqusi',
//     'last_name' : 'demo_sk8ac',
//     'email'     : '1ycmb@demo.com',
//     'created_at': '2016-11-17T13:03:32.000Z'
//   };
// therapists['0d4ba3c2-8747-4735-9cf7-197239ce1534'] = {
//     'first_name': 'demo_1udax',
//     'last_name' : 'demo_ryx7q',
//     'email'     : 'f9glr@demo.com',
//     'created_at': '2016-11-18T03:44:30.000Z'
//   };
var existingRowIds = [];

function therapistLogin() {
  connection = mysql.createConnection({
    host     : 'localhost.aptible.in',
    port     : '52978',
    user     : 'aptible',
    password : '4YAOqoQJZuit-uc6XjqTLhKY9x-ykRc8',
    database : 'db',
    ssl  : {
      // DO NOT DO THIS
      // set up your ca correctly to trust the connection
      rejectUnauthorized: false
    }
  });

  //var connection = mysql.createConnection("mysql://aptible:squDWAPcadiAkuLIcltCEE7T4cEP6SbM@127.0.0.1:58378/db");

  connection.connect();
}

function clientLogin() {
  client_connection = mysql.createConnection({
    host     : 'localhost.aptible.in',
    port     : '52948',
    user     : 'aptible',
    password : '8wrSdPE173ylVHK7jYb944QHp5CwrG4t',
    database : 'db',
    ssl  : {
      // DO NOT DO THIS
      // set up your ca correctly to trust the connection
      rejectUnauthorized: false
    }
  });

  //var connection = mysql.createConnection("mysql://aptible:squDWAPcadiAkuLIcltCEE7T4cEP6SbM@127.0.0.1:58378/db");

  client_connection.connect();
}

function logout() {
  connection.end();
  client_connection.end();
}

function addTherapist(therapist) {
  therapist.accepted = 0;
  therapist.pending = 0;
  therapists[therapist.id] = therapist;
}

function addInvite(invite) {
  invitations.push(invite);
}

function pullTherapistData(callback) {
    connection.query('SELECT id, first_name, last_name, email, created_at from users ORDER BY created_at', function (error, results, fields) {
      if (error) throw error;
      var counter = 0;
      results.forEach(function(therapist) {
          addTherapist(therapist);
          counter = counter + 1;
          //console.log(therapist.id, therapist.first_name, therapist.last_name, therapist.email, therapist.created_at);
      });
      console.log('Selected ', counter, ' therapists');
      callback();
    });
}

function pullClientData(callback) {
  client_connection.query('SELECT status, therapist_id FROM invitations', function (error, results, fields) {
    if (error) throw error;
    var counter = 0;
    results.forEach(function(invite) {
      addInvite(invite);
      counter = counter + 1;
    });
    console.log('Selected ', counter, ' invitations');
    callback();
  });
}

function updateCellIfChanged(row, v1, v2) {
  var rowValue = row[v1] == null ? def : row[v1];
  var inputVal = v2 == null ? '' : v2;

  if (v1 == 'ofclientsinvited' || v1 == 'ofclientsaccepted') {
    rowVal = rowVal == "" ? 0 : rowVal;
    inputVal = inputVal == "" ? 0 : inputVal;
  }

  if (v1 == 'dateofcreationofaccount') {
    rowVal = new Date(rowValue);
    tempVal = new Date(inputVal);
    rowMoment = moment(rowVal);
    inputVal = moment(tempVal);
    //if (rowVal.getTime() !== inputVal.valueOf() && rowValue !== inputVal.valueOf()) {
    if (rowMoment.format('MM/DD/YYYY') !== inputVal.format('MM/DD/YYYY')) {
      var d = inputVal.format('MM/DD/YYYY');
      console.log('Date Differ! ', v1, v2, rowMoment, inputVal, d);
      row[v1] = d;
      row.has_updated = 1;
    }
  } else {
    if (rowValue != inputVal) {
      console.log('Differ! ', v1, v2, rowValue, inputVal);
      row[v1] = inputVal;
      row.has_updated = 1;
    }
  }
}
//
// login();
// pullTherapistData();
// logout();

async.series([
  function setAuth(step) {
    // see notes below for authentication instructions!
    var creds = require('./crm-auth.json');
    // OR, if you cannot save the file locally (like on heroku)
    // var creds_json = {
    //   client_email: 'yourserviceaccountemailhere@google.com',
    //   private_key: 'your long private key stuff here'
    // }
    doc.useServiceAccountAuth(creds, step);
  },
  function getInfoAndWorksheets(step) {
    doc.getInfo(function(err, info) {
      console.log('Loaded doc: '+info.title+' by '+info.author.email);
      info.worksheets.forEach(function(worksheet) {
        if (worksheet.title === 'AUTOMATIC - Therachat Customers') {
          sheet = worksheet;
        }
      });
      console.log('sheet 1: '+sheet.title+' '+sheet.rowCount+'x'+sheet.colCount);
      step();
    });
  },
  function getTherapistData(step) {
    therapistLogin();

    function next() {
      step();
    }

    pullTherapistData(next);
  },
  function getClientData(step) {
    clientLogin();

    function next() {
      step();
    }

    pullClientData(next);
  },
  function aggregateInvitations(step) {
    invitations.forEach(function (invite) {
        if (therapists.hasOwnProperty(invite.therapist_id)) {
          if (invite.status === 'pending') {
            therapists[invite.therapist_id].pending = therapists[invite.therapist_id].pending + 1;
          }
          if (invite.status === 'accepted') {
            therapists[invite.therapist_id].accepted = therapists[invite.therapist_id].accepted + 1;
          }
        }
    });

    step();
  },
  function workingWithRows(step) {
    // google provides some query options
    sheet.getRows({
      offset: 1,
      limit: sheet.rowCount
      //orderby: 'col2'
    }, function( err, rows ){
      // console.log(therapists);
      // for (var k in therapists) {
      //   if (therapists.hasOwnProperty(k))
      //     console.log(k, therapists[k].first_name);
      // }

      console.log('Read '+rows.length+' rows');

      // Prepare
      var fixes = 0;
      rows.forEach(function(row) {
        if (row.unique === null || row.unique === undefined || row.unique === "") {
          for (var k in therapists) {
            if (therapists.hasOwnProperty(k)) {
              if (therapists[k].email === row.emailaddress) {
                row.unique = therapists[k].id;
                row.save();
                fixes = fixes + 1;
              }
            }
          }
        }
      });
      console.log('Fixed rows: ', fixes);
      if (fixes > 0) {
        step();
      } else {
        //console.log(rows);
        var updateCounter = 0;
        rows.forEach(function(row) {
          if (therapists.hasOwnProperty(row.unique)) {
            existingRowIds.push(row.unique);
            var therapist = therapists[row.unique];
            row.has_updated = 0;
            updateCellIfChanged(row, 'firstname', therapist.first_name);
            updateCellIfChanged(row, 'lastname', therapist.last_name);
            updateCellIfChanged(row, 'emailaddress', therapist.email);
            updateCellIfChanged(row, 'dateofcreationofaccount', therapist.created_at);
            updateCellIfChanged(row, 'ofclientsinvited', therapist.pending + therapist.accepted);
            updateCellIfChanged(row, 'ofclientsaccepted', therapist.accepted);

            if (row.has_updated === 1) {
              row.updated_at = Date().toLocaleString();
              updateCounter = updateCounter + 1;
              row.save();
            }
          }
          //console.log('Rowdat', row.unique);
        });
        console.log('Updated rows: ', updateCounter);

        //console.log(existingRowIds.length);
        // Pull items NOT in the sheet
        Object.keys(therapists).sort(function(a,b){return therapists[a].created_at-therapists[b].created_at});
        var counter = 0;
        for (var k in therapists) {
          if (therapists.hasOwnProperty(k)) {
            if (existingRowIds.indexOf(k) === -1) {
              //console.log('Not found: ', k);
              counter = counter + 1;
              var therapist = therapists[k];
              sheet.addRow({
                unique: therapist.id,
                firstname: therapist.first_name,
                lastname: therapist.last_name,
                emailaddress: therapist.email,
                dateofcreationofaccount: moment(therapist.created_at).format('MM/DD/YYYY'),
                ofclientsinvited: therapist.pending + therapist.accepted,
                ofclientsaccepted: therapist.accepted
              });
            }
          }
        }
        console.log('Added rows: ', counter);
        // the row is an object with keys set by the column headers
        // rows[0].colname = 'new val';
        // rows[0].save(); // this is async
        //
        // // deleting a row
        // rows[0].del();  // this is async

        step();
      }
    });
  },
  function logs(step) {
    logout();
    step();
  }
  // function workingWithCells(step) {
  //   sheet.getCells({
  //     'min-row': 1,
  //     'max-row': 5,
  //     'return-empty': true
  //   }, function(err, cells) {
  //     // var cell = cells[0];
  //     // console.log('Cell R'+cell.row+'C'+cell.col+' = '+cells.value);
  //
  //     // cells have a value, numericValue, and formula
  //     // cell.value == '1'
  //     // cell.numericValue == 1;
  //     // cell.formula == '=ROW()';
  //     //
  //     // // updating `value` is "smart" and generally handles things for you
  //     // cell.value = 123;
  //     // cell.value = '=A1+B2'
  //     // cell.save(); //async
  //     //
  //     // // bulk updates make it easy to update many cells at once
  //     // cells[0].value = 1;
  //     // cells[1].value = 2;
  //     // cells[2].formula = '=A1+B1';
  //     // sheet.bulkUpdateCells(cells); //async
  //
  //     step();
  //   });
  //}
  // function managingSheets(step) {
  //   doc.addWorksheet({
  //     title: 'my new sheet'
  //   }, function(err, sheet) {
  //
  //     // change a sheet's title
  //     sheet.setTitle('new title'); //async
  //
  //     //resize a sheet
  //     sheet.resize({rowCount: 50, colCount: 20}); //async
  //
  //     sheet.setHeaderRow(['name', 'age', 'phone']); //async
  //
  //     // removing a worksheet
  //     sheet.del(); //async
  //
  //     step();
  //   });
  // }
]);
