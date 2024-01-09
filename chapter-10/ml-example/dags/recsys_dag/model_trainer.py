import boto3
import polars as pl
import tensorflow as tf
import os
from pathlib import Path

from sklearn.model_selection import train_test_split



run_hash = os.environ.get('RUN_HASH',None)
data_set_key = os.environ.get('RECSYS_DATA_SET_KEY',None)

bucket_name = os.environ.get('RECSYS_BUCKET_NAME', 'recsys')

endpoint_url = os.environ.get('S3_ENDPOINT', "http://host.docker.internal:9000")
aws_access_key_id = os.environ.get("AWS_ACCESS_KEY", 'airflow123')
aws_secret_access_key = os.environ.get("AWS_SECRET_KEY", 'airflow123')

if not (run_hash and data_set_key):
    raise ValueError("RUN_HASH and RECSYS_DATA_SET_KEY environment variables must be set")

def main():

    s3 = boto3.resource('s3',
                    endpoint_url=endpoint_url,
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key
                    )

    download_location = Path.home() / 'ratings.csv'

    s3.Bucket(bucket_name).download_file(data_set_key, download_location)

    #Prepare our dataset
    ratings_df = pl.read_csv(download_location)
    ratings_df.drop_in_place("timestamp")
    ratings_df = ratings_df.with_columns(pl.col('movieId').rank(method="dense").alias("movie_encoding")-1)
    ratings_df = ratings_df.with_columns(pl.col('userId').rank(method="dense").alias("user_encoding")-1)

    # relevant metadata for our pre processing and model architecture
    n_users = ratings_df.n_unique(subset=["user_encoding"])
    n_movies = ratings_df.n_unique(subset=["movie_encoding"])
    max_rating = ratings_df['rating'].max()
    min_rating = ratings_df['rating'].min()

    # Test train splits and normalization
    X = ratings_df[['user_encoding', 'movie_encoding']].to_numpy()
    y = ratings_df['rating'].to_numpy()
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.1, random_state=50)
    X_train.shape, X_test.shape, y_train.shape, y_test.shape

    X_train_array = [X_train[:, 0], X_train[:, 1]]
    X_test_array = [X_test[:, 0], X_test[:, 1]]
    y_train = (y_train - min_rating)/(max_rating - min_rating)
    y_test = (y_test - min_rating)/(max_rating - min_rating)
    n_factors = 100

    # Create the model


    user = tf.keras.layers.Input(shape = (1,))

    u = tf.keras.layers.Embedding(n_users, n_factors, embeddings_initializer = 'he_normal', embeddings_regularizer = tf.keras.regularizers.l2(1e-6))(user)
    u = tf.keras.layers.Reshape((n_factors,))(u)

    movie = tf.keras.layers.Input(shape = (1,))

    m = tf.keras.layers.Embedding(n_movies, n_factors, embeddings_initializer = 'he_normal', embeddings_regularizer=tf.keras.regularizers.l2(1e-6))(movie)
    m = tf.keras.layers.Reshape((n_factors,))(m)

    x = tf.keras.layers.Concatenate()([u,m])
    x = tf.keras.layers.Dropout(0.05)(x)

    x = tf.keras.layers.Dense(32, kernel_initializer='he_normal')(x)
    x = tf.keras.layers.Activation(activation='relu')(x)
    x = tf.keras.layers.Dropout(0.05)(x)

    x = tf.keras.layers.Dense(16, kernel_initializer='he_normal')(x)
    x = tf.keras.layers.Activation(activation='relu')(x)
    x = tf.keras.layers.Dropout(0.05)(x)

    x = tf.keras.layers.Dense(9)(x)
    x = tf.keras.layers.Activation(activation='softmax')(x)

    model = tf.keras.models.Model(inputs=[user,movie], outputs=x)

    model.compile(optimizer='sgd', loss=tf.keras.losses.SparseCategoricalCrossentropy(), metrics=['accuracy'])

    #Train the model
    reduce_lr = tf.keras.callbacks.ReduceLROnPlateau(monitor='val_loss', factor=0.75, patience=3, min_lr=0.000001, verbose=1)

    model.fit(x = X_train_array, y = y_train, batch_size=4096, epochs=70, verbose=1, validation_data=(X_test_array, y_test),shuffle=True,callbacks=[reduce_lr])

    #Save locally
    local_model = Path.home() / "model.keras"
    model.save(local_model)

    #Upload to the remote bucket
    model_destination = f"{run_hash}/model.keras"
    s3.Bucket(bucket_name).upload_file(local_model, model_destination)

    #KPO XCOM
    with open("/airflow/xcom/return.json", "w") as f:
        f.write(f'"{model_destination}"')

    #DockerOperator XCOM
    print(f"{model_destination}")

    

if __name__ == '__main__':
    main()
