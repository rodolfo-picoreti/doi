const fs = require('fs');
const {parseFiles, unparse} = require('babyparse');
const _ = require('lodash');
const rp = require('request-promise');
const Bottleneck = require("bottleneck"); 
const wuzzy = require('wuzzy');

const REQUEST_MAX_PARALLELLISM = process.env.REQUEST_MAX_PARALLELLISM || 5;
const DELAY_BETWEEN_REQUESTS = process.env.DELAY_BETWEEN_REQUESTS || 500;
const AUTHOR_FIELD = process.env.AUTHOR_FIELD || "NomeAutor";
const CONTAINER_FIELD = process.env.CONTAINER_FIELD || "NomePeriodico";
const TITLE_FIELD = process.env.TITLE_FIELD || "Titulo";
const YEAR_FIELD = process.env.YEAR_FIELD || "Ano";
const CSV_DELIMITER = process.env.CSV_DELIMITER || ";";
const CSV_IN_PATH = process.argv[2] || "publications.csv";
const CSV_OUT_PATH = process.argv[3] || "publications_xref.csv";

const limiter = new Bottleneck(REQUEST_MAX_PARALLELLISM, DELAY_BETWEEN_REQUESTS);

let {data} = parseFiles(CSV_IN_PATH, { delimiter: CSV_DELIMITER });
let nonEmpty = _.filter(data, (record) => record.length > 1);
let fields = _.concat('Index', nonEmpty[0], 'DOI', 'Abstract');
let indexed = _.map(nonEmpty.slice(1), (array, index) => _.concat(index, array));
let groupped = _.groupBy(indexed, fields.indexOf(TITLE_FIELD));

function appendToFile(array) {
  fs.appendFileSync(CSV_OUT_PATH, unparse(array, {delimiter: ';'}) + ';\r\n'); 
};

function computeScore(our, rcv) {
  const scoreFunction = {
    title: (our, rcv) => wuzzy.levenshtein(our.toLowerCase(), rcv.toLowerCase()),
    container(our, rcv) {
      return wuzzy.levenshtein(our.toLowerCase(), rcv.toLowerCase())
    },
    year(our, rcv) {
      return our == rcv ? 1.5 : 0.5;
    },
    author(our, rcv) {
      return our.toLowerCase().indexOf(rcv.toLowerCase()) != -1 ? 1.5 : 0.5;
    }
  };

  let score = _.reduce(rcv, (total, value, key) => {
    if (_.isNull(value)) {
      return { score: total.score, nulls: _.concat(total.nulls, key) };
    }
    return { score: scoreFunction[key](our[key], value)*total.score, nulls: total.nulls };
  }, { score: 1.0, nulls: [] });
  
  if (!_.isEmpty(score.nulls)) {
    console.warn(`[!] "${our.title.slice(0,49)} [...]" fields ${_.join(score.nulls, ', ')} are undefined`);
  }
  
  return score.score * 100.0;
} 

appendToFile([fields]);

let found = 0;
let total = Object.keys(groupped).length;
console.log(`[x] Processing ${total} papers... This will take approx. ${total*DELAY_BETWEEN_REQUESTS/60000} m`);

Promise.all(_.map(groupped, (group) => {
  let our = {
    title: group[0][fields.indexOf(TITLE_FIELD)],
    container: group[0][fields.indexOf(CONTAINER_FIELD)],
    author: group[0][fields.indexOf(AUTHOR_FIELD)],
    year: group[0][fields.indexOf(YEAR_FIELD)]  
  };

  return limiter.schedule(rp, {
    uri: 'https://api.crossref.org/works',
    qs: {
      'query.title': our.title,
      'query.author': our.author,
      'rows': 1
    },
    json: true
  }).then((result) => {
    let {status, message:{items}} = result;
    let rcv = {
      title: _.get(items[0], 'title[0]', null),
      container: _.get(items[0], 'container-title[0]', null),
      year: _.get(items[0], 'issued.date-parts[0][0]', null),
      author: _.get(items[0], 'author[0].family', null)
    };

    let score = computeScore(our, rcv);
    console.log(`[${status}] "${our.title.slice(0,49)} [...]" ${score.toFixed(2)}`);
    
    if (score > 40.0) {
      ++found;
      group = _.map(group, (record) => {
        return _.concat(record, _.get(items[0], 'DOI', 'Not found'), _.get(items[0], 'abstract', 'Not found'));
      });
    } else {
      group = _.map(group, (record) => {
        return _.concat(record, 'Not found', 'Not found');
      });
    }

    appendToFile(group);
  });
})).catch((e) => {
  console.log(`[!] Exception: ${e}`);
}).then(() => {
  console.log(`[x] All done ... found ${found}/${total}`);
}); 
