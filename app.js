const express = require('express');
const cookieParser = require('cookie-parser');
const bodyParser = require('body-parser');
const cors = require('cors');

const { port } = require('./config');

const routes = require('./routes/index');

const acs_middleware = require('./acs-middleware');

const app = express();

acs_middleware.start();

// acs_middleware.start();

app.use(cors());
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
app.use(cookieParser());


app.use('/', routes);

// catch 404 and forward to error handler
app.use(function(req, res, next) {
  const err = new Error('Not Found');
  err.status = 404;
  next(err);
});

app.listen(port, function(){
  console.log('Server is running on Port:', port);
});