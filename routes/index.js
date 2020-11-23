const express = require('express');
const azureCtrl = require('../controllers/azure.controller');

var router = express.Router();

router.route('/')
  .post(azureCtrl.sendMessage)

module.exports = router;
