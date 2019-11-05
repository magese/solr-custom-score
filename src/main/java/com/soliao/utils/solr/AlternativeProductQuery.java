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
            // 获取查询请求加权字段
            String[] wgs = solrParams.getParams(WEIGHT);
            Set<String> weightList = generateWeightList(wgs);
            // 获取边界值map
            Map<String, Double> limitMap = generateLimitMap(queryParamMap);
            // 获取字段默认权重
            Map<String, Double> weightMap = generateDefaultWeightMap(limitMap);

            // 获取每个文档中用于评分的域
            double similarSum = 0;
            double idScore = 0;
            double weightScore = 100.0 / weightList.size();
            boolean fillersFlag = false;
            Document document = indexReader.document(doc);

            // 计算相似度得分
            for (Map.Entry<String, Double> entry : weightMap.entrySet()) {
                String fieldName = entry.getKey();

                Object beRep = queryParamMap.get(fieldName);
                IndexableField rep = document.getField(fieldName);

                double similar = 0;
                if ("fillers".equals(fieldName)) {
                    if ((beRep == null && rep == null) || (rep != null && rep.stringValue().equals(beRep))) {
                        similar = 1;
                        fillersFlag = true;
                    }
                } else if ("fillersContent".equals(fieldName) && fillersFlag && (beRep == null && rep == null) || (rep != null && rep.stringValue().equals(beRep))) {
                    similar = 1;
                } else if ("rep_fr".equals(fieldName) && rep != null && (double) beRep == rep.numericValue().doubleValue()) {
                    similar = 1;
                } else if (fieldName.startsWith("rep_") && rep != null) {
                    similar = getFieldSimilar(Double.parseDouble((String) beRep), rep.numericValue().doubleValue(), limitMap.get(fieldName));
                }

                similarSum += similar * weightMap.getOrDefault(fieldName, 0.0);
                if (weightList.contains(fieldName)) {
                    similarSum += getWeightScore(similar, weightScore);
                }
            }
            // 计算id得分
            if (queryParamMap.get("id") != null) {
                @SuppressWarnings("unchecked")
                List<String> ids = (List<String>) queryParamMap.get("id");
                String id = document.getField("id").stringValue();
                idScore += getIdScore(id, ids);
            }
            // 计算最终得分
            float customScore;
            if (limitMap.isEmpty()) {
                customScore = subQueryScore;
            } else {
                customScore = (float) (similarSum + idScore);
            }
            return customScore;
        }

        /**
         * 生成边界值映射
         *
         * @param queryParamMap 查询参数映射
         * @return 边界值映射
         */
        private Map<String, Double> generateLimitMap(Map<String, Object> queryParamMap) {
            Map<String, Double> limitMap = new LinkedHashMap<>();
            for (String key : queryParamMap.keySet()) {
                String[] params = solrParams.getParams(SUB_PRE + key);
                if (params != null)
                    limitMap.put(key, Double.valueOf(params[0]));
            }
            return limitMap;
        }

        /**
         * 生成默认的评分权重映射
         *
         * @param limitMap 边界值映射
         * @return 权重映射
         */
        private Map<String, Double> generateDefaultWeightMap(Map<String, Double> limitMap) {
            double residueScore = 0;
            // 自定义权重字段
            String[] customWeightArray = {"rep_mfr", "rep_mvr", "rep_density"};
            List<String> customWeightFields = new ArrayList<>(Arrays.asList(customWeightArray));
            // 权重map
            Map<String, Double> weightMap = new HashMap<>();
            weightMap.put("fillers", 15.0);
            weightMap.put("fillersContent", 15.0);
            weightMap.put("rep_mfr", 20.0);
            weightMap.put("rep_mvr", 20.0);
            weightMap.put("rep_density", 10.0);
            // 剔除被替代牌号没有的自定义字段
            for (String field : customWeightArray) {
                if (limitMap.get(field) == null) {
                    residueScore += weightMap.remove(field);
                    customWeightFields.remove(field);
                }
            }
            // 计算其它字段得分
            for (Map.Entry<String, Double> entry : limitMap.entrySet()) {
                if (customWeightFields.contains(entry.getKey())) continue;
                weightMap.put(entry.getKey(), 20.0 / (limitMap.size() - customWeightFields.size()));
            }
            // 如果被替代牌号无自定义分值字段，则将该字段分值平均分配给其它字段
            if (residueScore > 0) {
                double avgScore = residueScore / weightMap.size();
                weightMap.replaceAll((k, v) -> v + avgScore);
            }
            return weightMap;
        }

        /**
         * 获取加权字段得分
         *
         * @param similar     相似度
         * @param weightScore 加权分数
         * @return 加权得分
         */
        private double getWeightScore(double similar, double weightScore) {
            double score;
            if (similar == 1) {
                score = weightScore * similar;
            } else if (similar > 0.9) {
                score = weightScore / 2 * similar;
            } else if (similar > 0.8) {
                score = weightScore / (2 * 2) * similar;
            } else if (similar > 0.7) {
                score = weightScore / (2 * 3) * similar;
            } else if (similar > 0.6) {
                score = weightScore / (2 * 4) * similar;
            } else if (similar > 0.5) {
                score = weightScore / (2 * 5) * similar;
            } else if (similar > 0.4) {
                score = weightScore / (2 * 6) * similar;
            } else if (similar > 0.3) {
                score = weightScore / (2 * 7) * similar;
            } else if (similar > 0.2) {
                score = weightScore / (2 * 8) * similar;
            } else if (similar > 0.1) {
                score = weightScore / (2 * 9) * similar;
            } else {
                score = weightScore / (2 * 10) * similar;
            }
            return score;
        }

        /**
         * 获取id加权的评分结果
         *
         * @param docId 文档id
         * @param ids   加权id集合
         * @return 最终得分
         */
        private double getIdScore(String docId, List<String> ids) {
            double idScore = 0;
            double multiple = 1D;
            for (String id : ids) {
                if (id.contains("^")) {
                    String[] split = id.split("\\^");
                    id = split[0];
                    multiple = Double.parseDouble(split[1]);
                }
                if (id.equals(docId))
                    idScore = 1 * multiple;
            }
            return idScore;
        }

        /**
         * 获取字段相似度
         *
         * @param beRep      被替代字段数值
         * @param rep        替代字段数值
         * @param limitValue 边界值
         * @return 该字段相似度
         */
        private double getFieldSimilar(double beRep, double rep, double limitValue) {
            double max = Math.max(beRep, rep);
            double sub = max - Math.min(beRep, rep);

            double similar;
            if (sub == 0) similar = 1;
            else if (sub > limitValue) similar = 0;
            else if (max != 0) similar = 1 - sub / limitValue;
            else similar = 0;

            return similar;
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
        private Set<String> generateWeightList(String[] params) {
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

