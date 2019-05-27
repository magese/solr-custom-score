package com.soliao.utils.solr;

import org.apache.lucene.search.Query;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.SyntaxError;

public class AlternativeProductQParserPlugin extends QParserPlugin {

    public QParser createParser(String s, SolrParams solrParams, SolrParams solrParams1, SolrQueryRequest solrQueryRequest) {
        return new AlternativeProductParser(s, solrParams, solrParams1, solrQueryRequest);
    }

    private static final class AlternativeProductParser extends QParser {
        private Query innerQuery;

        public AlternativeProductParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
            super(qstr, localParams, params, req);
            QParser parser;
            try {
                parser = getParser(qstr, "lucene", getReq());
                this.innerQuery = parser.getQuery();
            } catch (SyntaxError syntaxError) {
                throw new RuntimeException("error parsing query", syntaxError);
            }
        }

        public Query parse() throws SyntaxError {
            return new AlternativeProductQuery(innerQuery, getParams());
        }
    }
}
