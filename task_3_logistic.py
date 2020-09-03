from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import HashingTF, Tokenizer, IDF, StopWordsRemover
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
import subprocess

from pyspark.ml.classification import LogisticRegression, OneVsRest

# Index labels, adding metadata to the label column.
# Fit on whole dataset to include all labels in index.
labelIndexer = StringIndexer(inputCol="category_code", outputCol="label").fit(data)

tokenizer = Tokenizer(inputCol="descr", outputCol="words")
wordsData = tokenizer.transform(data)

# Automatically identify categorical features, and index them.
# hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="features")
hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="rawFeatures")
featurizedData = hashingTF.transform(wordsData)

idf = IDF(inputCol="rawFeatures", outputCol="features")

(train, test) = data.randomSplit([0.7, 0.3])
train.cache()

lr = LogisticRegression(maxIter=10, tol=1E-6, fitIntercept=True)

ovr = OneVsRest(classifier=lr)

pipeline = Pipeline(stages=[labelIndexer, tokenizer, hashingTF,idf , ovr])


paramGrid = ParamGridBuilder() \
    .addGrid(hashingTF.numFeatures, [10, 1000, 10000]) \
    .build()
crossval = CrossValidatorVerbose(estimator=pipeline,
                      estimatorParamMaps=paramGrid,
                      evaluator=MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy"),
                      numFolds=5) 

# train the multiclass model.
ovrModel = crossval.fit(train)

# score the model on test data.
predictions = ovrModel.transform(test)

# obtain evaluator.
evaluator = MulticlassClassificationEvaluator(metricName="accuracy")

# compute the classification error on test data.
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g" % (1.0 - accuracy))