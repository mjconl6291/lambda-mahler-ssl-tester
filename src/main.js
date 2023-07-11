const https = require('https');
const fs = require('fs');

const url = 'https://bosfwtkrc5.execute-api.us-east-2.amazonaws.com/prod/patient-update';
let data = {
  "client_id": "418136",
  "modified": "2023-07-10 05:42:48",
  "fname": "Maura",
  "lname": "Patient",
  "marital_status": "0",
  "address1": "123 Main Street",
  "address2": "Apt 4B",
  "city": "",
  "state": "Stateville",
  "zip": "12345",
  "homephone": "555-123-4567",
  "cellphone": "555-555-5555",
  "workphone": "555-987-6543",
  "ssn": "123-45-6789",
  "bdate": "1955-01-22 00:00:00",
  "age": "68",
  "primarycarephysician": "Dr. Smith",
  "employer": "ABC Company",
  "responsible_bill": "self",
  "partner_name": "John Doe",
  "partner_phone": "555-111-2222",
  "emergency_contact": "Jane Smith",
  "pinsurance": "AETNA",
  "pinsurance_master_payer_id": "2760",
  "pinsurance_number": "W4829429585",
  "pinsurance_group": "TROYWASHERE",
  "pinsurance_dob": "1955-01-22 00:00:00",
  "pinsurance_insurance_type": "Health",
  "sinsurance": "Blue Cross",
  "sinsurance_master_payer_id": "1234",
  "sinsurance_number": "S123456789",
  "sinsurance_group": "GROUP123",
  "sinsurance_dob": "1970-05-15 00:00:00",
  "sinsurance_insurance_type": "Dental",
  "medicaid": "123456789",
  "no_insurance": "0",
  "pinsurance_payerID": "60054",
  "sinsurance_payerID": "98765",
  "pinsurance_lastname": "Patient",
  "pinsurance_firstname": "Maura",
  "sinsurance_lastname": "Doe",
  "sinsurance_firstname": "John",
  "practice_id": "123",
  "tertiary_payer_name": "Cigna",
  "tertiary_master_payer_id": "5678",
  "tertiary_payer_id": "9999",
  "tertiary_subscriber_id": "S888888",
  "tertiary_group_id": "GROUP999",
  "tertiary_lastname": "Smith",
  "tertiary_firstname": "Jane",
  "tertiary_dob": "1980-10-25 00:00:00",
  "tertiary_insurance_type": "Vision",
  "male_female": "F",
  "additional_account_number": "1",
  "detailed_notes": "This is a test note.",
  "is_hispanic": "0",
  "client_race": "0",
  "is_hospice": "0",
  "guarentor_lastname": "Patient",
  "guarentor_firstname": "Maura",
  "guarentor_address1": "123 Main Street",
  "guarentor_address2": "Apt 4B",
  "guarentor_city": "Cityville",
  "guarentor_state": "Stateville",
  "guarentor_zip": "12345",
  "primary_mapped_id": "987",
  "secondary_mapped_id": "654",
  "tertiary_mapped_id": "321"
};

data = JSON.stringify(data);
const pemKeyPath = `${process.env.HOME}/Documents/Keys/mahler.pem`;

const options = {
    hostname: 'your-host-name', //Replace with your invocation URL
    port: 443,
    path: '/your/route', //Replace with your route
    method: 'POST',
    headers: {
      'x-api-key': 'your-api-key' // Replace 'your-api-key' with the actual API key value
    },
    // cert: fs.readFileSync(pemKeyPath),
    rejectUnauthorized: true
  };

const req = https.request(options, (res) => {
  console.log(`Status Code: ${res.statusCode}`);
  res.setEncoding('utf8');
  res.on('data', (chunk) => {
    console.log(`Response: ${chunk}`);
  });
});

req.on('error', (error) => {
  console.error('Error:', error);
});

req.write(data);
req.end();
