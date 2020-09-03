from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import HashingTF, Tokenizer, IDF, StopWordsRemover
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
import subprocess

from pyspark.ml.classification import RandomForestClassifier

# Index labels, adding metadata to the label column.
# Fit on whole dataset to include all labels in index.
labelIndexer = StringIndexer(inputCol="category_code", outputCol="indexedLabel").fit(data)

tokenizer = Tokenizer(inputCol="descr", outputCol="words")
# wordsData = tokenizer.transform(data)

# Automatically identify categorical features, and index them.
hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="rawFeatures")
# featurizedData = hashingTF.transform(wordsData)

idf = IDF(inputCol="rawFeatures", outputCol="features")

labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel",
                               labels=labelIndexer.labels)

rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol='features', numTrees=10)

pipeline = Pipeline(stages=[labelIndexer, tokenizer, hashingTF, idf,  rf, labelConverter])

(train, test) = data.randomSplit([0.7, 0.3])
train.cache()

paramGrid = ParamGridBuilder() \
    .addGrid(hashingTF.numFeatures, [10, 1000, 10000]) \
    .addGrid(rf.numTrees, [10, 20]) \
    .build()

crossval = CrossValidatorVerbose(estimator=pipeline,
                      estimatorParamMaps=paramGrid,
                      evaluator=MulticlassClassificationEvaluator(labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy"),
                      numFolds=5) 

rf_model = crossval.fit(train)

prediction = rf_model.transform(test)

evaluator = MulticlassClassificationEvaluator(labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")

accuracy = evaluator.evaluate(prediction)
print("Test Error = %g " % (1.0 - accuracy))
