# song-recommendation

# **User-Based Collaborative Filtering Recommendation System**

## **Overview**

This project implements a **User-Based Collaborative Filtering** recommendation system using **Apache Spark**. The system makes personalized song recommendations for a user based on the listening behavior of similar users. The core idea is that if two users have similar listening patterns (i.e., they listen to the same songs), the system will recommend songs that one user has listened to, but the other user has not yet discovered.

This approach follows the **collaborative filtering** paradigm, which is commonly used in recommendation systems, where recommendations are made based on the past behavior of other users, rather than the attributes of the items themselves.

## **How it Works**

The system works in the following steps:

1. **Identifying the Target User:**
   - A user ID is provided as input (via command-line arguments), and the system selects this user as the **target user** for recommendations.

2. **Selecting Top Songs of the Target User:**
   - The model first selects the top `threshold_songpick` songs based on the target user’s listening history, i.e., the songs the target user has listened to the most.

3. **Finding Similar Users:**
   - The system identifies other users who have listened to the same songs as the target user. These users are considered to be **"similar"** to the target user.
   - The more songs two users have in common (with similar listening counts), the more similar they are considered.

4. **Calculating User Weights:**
   - A weight is assigned to each similar user based on how frequently they have listened to the same songs as the target user.
   - The system computes a **user weight** for each similar user, where users with more overlaps in their listening history receive a higher weight.

5. **Generating Recommendations:**
   - The system generates a list of recommended songs by aggregating the weights of similar users' interactions with songs that the target user hasn’t yet listened to.
   - The final recommendations are then sorted by their relevance, with higher-ranked songs being recommended first.

## **Key Concepts**

### **User-Based Collaborative Filtering**
In **user-based collaborative filtering**, recommendations are made based on the behavior of users who are similar to the target user. The system assumes that if users A and B have listened to the same songs in the past, they are likely to have similar tastes, and thus, songs liked by user B but not yet listened to by user A can be recommended.

### **Spark’s Role**
This system is implemented using **Apache Spark**, which provides distributed computing capabilities. Spark helps to handle large datasets by performing operations like **map, groupBy, reduce, join**, and **filter** efficiently in parallel across a cluster.

### **Challenges in User-Based Collaborative Filtering**
- **Scalability:** As the number of users and songs increases, the computation for finding similar users becomes expensive.
- **Sparsity:** If not enough users have interacted with the same songs, the system may struggle to find sufficient similarity between users.
- **Cold Start Problem:** New users with little to no interaction history may not be able to receive good recommendations.
- **Popularity Bias:** Popular songs are more likely to be recommended, leading to a lack of diversity in recommendations.

## **How to Use**

1. **Set up Spark:**
   - Ensure you have **Apache Spark** installed and properly configured. The system is designed to run locally or on a cluster (HDFS is used for file storage).
   - You can configure Spark settings using the `SparkSession.builder` in the code.

2. **Input Data:**
   - **Triplets Data (`train_triplets.txt`)**: This file contains user-song interactions, with columns for `user_id`, `song_id`, and `listen_count`. It is stored on HDFS.
   - **Song Data (`song_data.csv`)**: This file contains metadata about the songs, such as the song name, release year, etc. It is also stored on HDFS.
   - Make sure to place the correct paths for these files in the `df_triplets_fileloc` and `df_songData_name` variables.

3. **Running the Code:**
   - The code takes the user ID for which recommendations should be generated as a command-line argument:
     ```bash
     python recommender.py <user_id>
     ```
     Replace `<user_id>` with the actual user ID.

4. **Output:**
   - The output will display the top recommended songs for the given user based on their listening behavior and the behavior of similar users.

## **How It Works Internally**

1. **Mapping User and Song IDs:**
   - The system transforms the original user and song IDs into new unique IDs using a `UDF` (user-defined function). This allows for efficient processing of the data in Spark.

2. **Calculating User Weights:**
   - For each user, the system calculates a weight based on how frequently they have listened to the same songs as the target user.
   - These weights are computed by looking at the intersection of songs between users and aggregating the `listen_count` values.

3. **Generating Final Recommendations:**
   - After computing the user weights, the system generates a final list of recommended songs by joining the data on the users' song interactions and ordering the songs based on their relevance and listen count.

4. **Sorting and Filtering:**
   - The final recommended songs are sorted by their weighted score and filtered to return only the most relevant songs.

## **Limitations and Challenges**

While this system follows the user-based collaborative filtering approach, there are some **challenges**:
- **Scalability Issues:** As the number of users and songs grows, the computation required to find similarities between users can become very expensive.
- **Data Sparsity:** If the dataset is sparse (few users listen to the same songs), the system might not find many similar users, making recommendations less reliable.
- **Cold Start Problem:** New users with little listening history may not have enough data to find similar users, making recommendations difficult.
- **Bias Toward Popular Songs:** Popular songs may dominate the recommendations, leading to less diversity in the recommended songs.

## **Potential Improvements**

To address some of the challenges mentioned above, here are a few possible improvements:
- **Matrix Factorization / Latent Factor Models:** Techniques like Singular Value Decomposition (SVD) could help improve scalability and handle sparsity.
- **Hybrid Approaches:** Combining user-based collaborative filtering with item-based collaborative filtering or content-based filtering could enhance the quality and diversity of recommendations.

## **Conclusion**

This recommendation system uses **user-based collaborative filtering** to make personalized song recommendations based on the listening behavior of similar users. While simple, it demonstrates the core principles of collaborative filtering and can be scaled using Apache Spark. However, further optimizations such as matrix factorization or hybrid models can help improve scalability, accuracy, and diversity in recommendations.
