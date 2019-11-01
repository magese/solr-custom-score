package com.soliao.utils.solr;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.CustomScoreProvider;
import org.apache.lucene.queries.CustomScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.common.params.SolrParams;

import java.io.IOException;
import java.util.*;

public class AlternativeProductQuery extends CustomScoreQuery {
    private SolrParams solrParams;

    public AlternativeProductQuery(Query subQuery, SolrParams params) {
        super(subQuery);
        this.solrParams = params;
    }

    public AlternativeProductQuery(Query subQuery) {
        super(subQuery);
    }

    @Override
    protected CustomScoreProvider getCustomScoreProvider(LeafReaderContext context) throws IOException {
        return new AlternativeProductScoreProvider(context);
    }

    public SolrParams getSolrParams() {
        return solrParams;
    }

    class AlternativeProductScoreProvider extends CustomScoreProvider {
        public AlternativeProductScoreProvider(LeafReaderContext context) {
            super(context);
        }

        // 最大值与最小值差值参数前缀
        private static final String SUB_PRE = "sub.";
        // 加权字段
        private static final String WEIGHT = "wg.f";

        /**
         * 牌号替代相似度文档计分
         *
         * @param doc           文档编号
         * @param subQueryScore 原始分数
         * @param valSrcScores  不知道是啥
         * @return 自定义分数
         * @throws IOException 读取异常
         */
        @Override
        public float customScore(int doc, float subQueryScore, float[] valSrcScores) throws IOException {
            IndexReader indexReader = this.context.reader();
            SolrParams solrParams = getSolrParams();
            // 获取查询请求参数q
            String[] queryParams = solrParams.getParams("q");
            Map<String, Object> queryParamMap = generateQueryMap(queryParams);
            // 获取查询请求wd
            String[] wgs = solrParams.getParams(WEIGHT);
            Set<String> weightList = generateWidthList(wgs);

            // 获取最大值与最小值的差值map
            Map<String, Double> subMap = new LinkedHashMap<>();
            for (String key : queryParamMap.keySet()) {
                String[] params = solrParams.getParams(SUB_PRE + key);
                if (params != null)
                    subMap.put(key, Double.valueOf(params[0]));
            }

            // 获取每个文档中用于评分的域
            double similarSum = 0;
            double idScore = 0;
            boolean fillersFlag = false;
            Document document = indexReader.document(doc);
            for (Map.Entry<String, Object> entry : queryParamMap.entrySet()) {
                String qname = entry.getKey();

                // 如果为不评分字段或无该字段则跳过当次循环
                if (!(qname.startsWith("rep_") ||
                        "id".equals(qname) ||
                        "fillers".equals(qname) ||
                        "fillersContent".equals(qname))) continue;
                IndexableField field = document.getField(qname);
                if (field == null) continue;

                // 定义相似值
                if ("id".equals(qname)) {
                    // 如果查询参数中有id字段
                    double multiple = 1D;
                    @SuppressWarnings("unchecked")
                    List<String> ids = (List<String>) entry.getValue();
                    for (String id : ids) {
                        if (id.contains("^")) {
                            String[] split = id.split("\\^");
                            id = split[0];
                            multiple = Double.parseDouble(split[1]);
                        }
                        if (id.equals(field.stringValue()))
                            idScore += 1 * multiple;
                    }
                } else if ("fillers".equals(qname)) {
                    String beRep = (String) entry.getValue();
                    String rep = field.stringValue();
                    if (beRep.equals(rep)) {
                        fillersFlag = true;
                        similarSum += 15;
                    }
                } else if ("fillersContent".equals(qname)) {
                    String beRep = (String) entry.getValue();
                    String rep = field.stringValue();
                    if (beRep.equals(rep) && fillersFlag)
                        similarSum += 15;
                } else if ("rep_mfr".equals(qname)) {
                    double maxSub = 2;
                    double score = 20;
                    double beRep = Double.parseDouble((String) entry.getValue());
                    double rep = field.numericValue().doubleValue();
                    similarSum += getFieldScore(beRep, rep, maxSub, score);
                } else if ("rep_mvr".equals(qname)) {
                    double maxSub = 2;
                    double score = 20;
                    double beRep = Double.parseDouble((String) entry.getValue());
                    double rep = field.numericValue().doubleValue();
                    similarSum += getFieldScore(beRep, rep, maxSub, score);
                } else if ("rep_density".equals(qname)) {
                    double maxSub = 2;
                    double score = 10;
                    double beRep = Double.parseDouble((String) entry.getValue());
                    double rep = field.numericValue().doubleValue();
                    similarSum += getFieldScore(beRep, rep, maxSub, score);
                } else if ("rep_fr".equals(qname)) {
                    double score = 20.0 / subMap.size();
                    double beRep = Double.parseDouble((String) entry.getValue());
                    double rep = field.numericValue().doubleValue();
                    if (beRep == rep)
                        similarSum += score;
                } else if (subMap.containsKey(qname)) {
                    // 如果字段为数值类型，则对该数值进行相似度计算
                    double beRep = Double.parseDouble((String) entry.getValue());
                    double rep = field.numericValue().doubleValue();
                    double maxSub = Math.max(beRep, rep);
                    double score = 20.0 / subMap.size();
                    similarSum += getFieldScore(beRep, rep, maxSub, score);
                    /*double max = Math.max(beRep, rep);
                    double sub = max - Math.min(beRep, rep);
                    double value = subMap.get(qname);
                    if (sub == 0) similar = 1;
                    else if (sub > max || "rep_fr".equals(qname)) similar = 0;
                    else if (max != 0) similar = 1 - sub / max;
                    else similar = 0;*/

                }

                /*if (weightList.isEmpty()) {
                    similarSum += similar * (1D / subMap.size()) * 100D;
                } else {
                    similarSum += similar * (1D / subMap.size()) * 0.01D;
                    if (weightList.contains(qname))
                        similarSum += similar * (1D / weightList.size()) * 99.99D;
                }*/
            }
            float customScore;
            if (subMap.isEmpty()) {
                customScore = subQueryScore;
            } else {
                customScore = (float) (similarSum + idScore);
            }
            return customScore;
        }

        /**
         * 获取字段的得分
         *
         * @param beRep  被替代字段数值
         * @param rep    替代字段数值
         * @param maxSub 最大差值
         * @param score  该字段分数
         * @return 该字段最终得分
         */
        private double getFieldScore(double beRep, double rep, double maxSub, double score) {
            double max = Math.max(beRep, rep);
            double sub = max - Math.min(beRep, rep);
            if (sub > maxSub) return 0;

            double similar;
            if (sub == 0) similar = 1;
            else if (sub > max) similar = 0;
            else if (max != 0) similar = 1 - sub / max;
            else similar = 0;

            return score * similar;
        }

        @Override
        public float customScore(int doc, float subQueryScore, float valSrcScore) throws IOException {
            return this.customScore(doc, subQueryScore, new float[]{valSrcScore});
        }

        /**
         * 解析参数生成map
         *
         * @param params 参数数组
         * @return 参数map
         */
        private Map<String, Object> generateQueryMap(String[] params) {
            Map<String, Object> paramsMap = new LinkedHashMap<>();
            if (params != null) {
                for (String param : params) {
                    String[] splitedParams = param.split("\\s");
                    for (String splitedParam : splitedParams) {
                        try {
                            if (StringUtils.isNotBlank(splitedParam) && splitedParam.contains(":")) {
                                String[] split = splitedParam.split(":");
                                if (split[1].contains("\""))
                                    split[1] = split[1].replace("\"", "");
                                if ("id".equals(split[0])) {
                                    @SuppressWarnings("unchecked")
                                    List<String> ids = (List<String>) paramsMap.computeIfAbsent("id", k -> new ArrayList<>());
                                    ids.add(split[1]);
                                } else {
                                    paramsMap.put(split[0], split[1]);
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
            return paramsMap;
        }

        /**
         * 解析参数生成加权字段Set
         *
         * @param params 参数数组
         * @return 加权字段集合
         */
        private Set<String> generateWidthList(String[] params) {
            Set<String> res = new LinkedHashSet<>();
            if (params != null) {
                for (String param : params) {
                    if (StringUtils.isNotBlank(param))
                        res.add(param.trim());
                }
            }
            return res;
        }

    }
}

