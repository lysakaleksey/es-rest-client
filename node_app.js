const express = require('express');
const app = express();
const port = 9200;
const timeout = 120;

app.get('/:indexName/:typeName/:id', (req, res) => {
  var body = {};
  body._index = req.params.indexName;
  body._type = req.params.typeName;
  body._id = req.params.id;
  body._version = 1;
  body.found = true;
  body._source = {"foo": "bar"};

  if (body._id === "0") {
    setTimeout((() => {
      res.send(body);
    }), timeout * 1000);
  } else {
    res.send(body);
  }

});

app.listen(port, () => console.log(`ES stub started on port ${port} with ${timeout} sec timeout`));
