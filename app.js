const fs = require('fs');
const {parseFiles, unparse} = require('babyparse');
const _ = require('lodash');
const rp = require('request-promise');
const Bottleneck = require("bottleneck"); 
const wuzzy = require('wuzzy');

const limiter = new Bottleneck(5, 2000);

let {data} = parseFiles(__dirname+'/pubs.csv', { delimiter: ";" });
data = _.filter(data, (record) => record.length > 1);

let fields = data[0];
fields.push('DOI');

let indexed = _.map(data.slice(1), (array, index) => _.concat(index, array));
let unique = _.uniqBy(indexed, fields.indexOf('Titulo') + 1);

console.log(`Found ${unique.length} unique titles`);

_.map(unique, (record) => {
  let index = record[0];
  let title = record[fields.indexOf('Titulo') + 1];
  let author = record[fields.indexOf('Nome Autor') + 1];
  let year = record[fields.indexOf('Ano') + 1];
  let code = record[fields.indexOf('Codigo Trabalho') + 1];
   
  limiter.schedule(rp, {
    uri: 'https://api.crossref.org/works',
    qs: {
      'query.title': title,
      'query.author': author,
      'rows': 1
    },
    json: true
  }).then((result) => {
    let {status, message:{items}} = result;
    let similarity = wuzzy.levenshtein(title, items[0].title[0]);
    data[index+1].push(items[0].DOI);
    data[index+1].push(similarity);
    console.log(`[${index}][${code}] ${title.slice(0,49)}... => (${status}, ${similarity})`);
  });
});

limiter.on('idle', () => {
  console.log(`All done... Saving`);
  fs.writeFileSync('pubs2.csv', unparse(data, {delimiter: ';'}));
});
