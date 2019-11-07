package com.itheima;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.itheima.domian.Article;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;


import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.Map;

public class EsTest {
    private TransportClient client;

    @Before
    public void init() throws UnknownHostException {
        Settings settings = Settings.builder ().put ( "cluster.name", "elasticsearch" ).build ();
        client = new PreBuiltTransportClient ( settings ).addTransportAddress ( new InetSocketTransportAddress ( InetAddress.getByName ( "127.0.0.1" ), 9301 ) );
        client = new PreBuiltTransportClient ( settings ).addTransportAddress ( new InetSocketTransportAddress ( InetAddress.getByName ( "127.0.0.1" ), 9302 ) );
        client = new PreBuiltTransportClient ( settings ).addTransportAddress ( new InetSocketTransportAddress ( InetAddress.getByName ( "127.0.0.1" ), 9303 ) );
    }

    @Test
    public void demo() throws Exception {
        client.admin ().indices ().prepareCreate ( "blog2" ).get ();
        client.close ();
    }

    @Test
    public void setting() throws Exception {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder ().startObject ().startObject ( "article" ).startObject ( "properties" ).
                startObject ( "id" ).field ( "type", "long" ).field ( "store", true ).endObject ().
                startObject ( "title" ).field ( "type", "text" ).field ( "store", true ).field ( "analyzer", "ik_max_word" ).endObject ()
                .startObject ( "content" ).field ( "type", "text" ).field ( "store", true ).field ( "analyzer", "ik_max_word" )
                .endObject ().endObject ().endObject ().endObject ();
        PutMappingRequest mapping = Requests.putMappingRequest ( "blog2" ).type ( "article" ).source ( xContentBuilder );
        client.admin ().indices ().putMapping ( mapping ).get ();
        client.close ();
    }

    @Test
    public void testAddDocument() throws IOException {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder ().startObject ().field ( "id", 1l ).field ( "title", "先擦鼻涕后提裤" )
                .field ( "content", "从此走上社会路" ).endObject ();
        client.prepareIndex ( "blog2", "article", "1" ).setSource ( xContentBuilder ).get ();
        client.close ();
    }

    @Test
    public void testAddDoucument2() throws JsonProcessingException {
        Article article = new Article ();
        article.setId ( 2 );
        article.setTitle ( "搜索工作其实很快乐" );
        article.setContent ( "我们希望我们的搜索解决方案要快，我们希望有一个零配置和一个完全免费的搜索模式， " +
                "我们希望能够简单地使用JSON通过HTTP的索引数据，我们希望我们的搜索服务器始终可用，" +
                "我们希望能够一台开始并扩 展到数百，我们要实时搜索，我们要简单的多租户，我们希望建立一个云的解决方案。" +
                "Elasticsearch旨在解决所有这 些问题和更多的问题" );
        ObjectMapper mapper = new ObjectMapper ();
        String s = mapper.writeValueAsString ( article );
        client.prepareIndex ( "blog2", "article", "2" ).setSource ( s, XContentType.JSON ).get ();
        client.close ();
    }

    @Test
    public void demo1SeartchById() throws Exception {
        QueryBuilder queryBuilder = QueryBuilders.idsQuery ().addIds ( "1", "2" );
        SearchResponse searchResponse = client.prepareSearch ( "blog2" ).setTypes ( "article" ).setQuery ( queryBuilder ).get ();
        SearchHits hits = searchResponse.getHits ();
        System.out.println ( hits.getTotalHits () );
        Iterator<SearchHit> iterator = hits.iterator ();
        while (iterator.hasNext ()) {
            SearchHit next = iterator.next ();
            System.out.println ( next.getSourceAsString () );
            Map<String, Object> source = next.getSource ();
            System.out.println ( source.get ( "id" ) );
            System.out.println ( source.get ( "title" ) );
            System.out.println ( source.get ( "content" ) );


        }
        client.close ();
    }

    @Test
    public void demo3() {
        SearchResponse searchResponse = client.prepareSearch ( "blog2" ).setTypes ( "article" ).setQuery ( QueryBuilders.queryStringQuery ( "路" ) ).get ();
        SearchHits hits = searchResponse.getHits ();
        System.out.println ( hits.getTotalHits () );
        for (SearchHit hit : hits) {
            System.out.println ( hit.getSourceAsString () );
        }
        client.close ();
    }

    @Test
    public void demo4() {
        SearchResponse searchResponse = client.prepareSearch ( "blog2" ).setTypes ( "article" ).setQuery ( QueryBuilders.termQuery ( "content", "路" ) ).get ();
        long totalHits = searchResponse.getHits ().getTotalHits ();
        System.out.println ( totalHits );
        for (SearchHit hit : searchResponse.getHits ()) {
            System.out.println ( hit.getSourceAsString () );
        }
        client.close ();
    }

    @Test
    public void InsertValue() throws IOException {
        for (int i = 0; i < 100; i++) {
            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder ().startObject ().field ( "id", i + 3 ).field ( "title", "社会摇小黄毛" + i ).field ( "content", "一扭一晃真像样" + i ).endObject ();
            client.prepareIndex ( "blog2", "article", String.valueOf ( i + 3 ) ).setSource ( xContentBuilder ).get ();
        }
        client.close ();
    }

    @Test
    public void demo5() {
        QueryStringQueryBuilder queryStringQueryBuilder = QueryBuilders.queryStringQuery ( "社会" ).defaultField ( "title" );
        SearchResponse searchRequestBuilder = client.prepareSearch ( "blog2" ).setTypes ( "article" ).setQuery ( queryStringQueryBuilder ).setFrom ( 0 ).setSize ( 5 ).get ();
        SearchHits hits = searchRequestBuilder.getHits ();
        for (SearchHit hit : hits) {
            Map<String, Object> source = hit.getSource ();
            System.out.println ( source.get ( "id" ) );
            System.out.println ( source.get ( "title" ) );
            System.out.println ( source.get ( "content" ) );
            System.out.println ("------------------------------------");
            Map<String, HighlightField> highlightFields = hit.getHighlightFields ();
            System.out.println (highlightFields);
        }
        System.out.println ();
        long totalHits = searchRequestBuilder.getHits ().getTotalHits ();
        System.out.println ( totalHits );
    }

    @Test
    public void demo6() throws IOException {
        QueryStringQueryBuilder queryStringQueryBuilder = QueryBuilders.queryStringQuery ( "社会摇" ).defaultField ( "title" );
        search ( queryStringQueryBuilder,"title" );

    }
    private void search(QueryBuilder queryBuilder,String highlightField){
        HighlightBuilder highlightBuilder = new HighlightBuilder ();
        highlightBuilder.field (highlightField );
        highlightBuilder.preTags ( "<em>" );
        highlightBuilder.postTags ( "</em>" );
        SearchResponse searchResponse = client.prepareSearch ( "blog2" ).setTypes ( "article" ).setQuery ( queryBuilder ).highlighter ( highlightBuilder ).get ();
        for (SearchHit hit : searchResponse.getHits ()) {
            Map<String, HighlightField> highlightFields = hit.getHighlightFields ();
            System.out.println (highlightFields);
            HighlightField highlightField1 = highlightFields.get ( highlightField );
            Text[] fragments = highlightField1.getFragments ();
            String s = fragments[0].toString ();
            System.out.println (s);
        }
        client.close ();
    }

}
