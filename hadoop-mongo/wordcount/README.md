[ short/WordCount.java ]
설명 : movies.csv 의 장르를 카운트 합니다.
실행 : hadoop jar WordCount.jar WordCount [마스터노드의 내부아이피]/[db이름].[collection이름] [마스터노드의 내부아이피]/[db이름].[collection이름]
예제 : hadoop jar WordCount.jar WordCount 10.146.0.8/dataset.movielens 10.146.0.8/dataset.out

[ long/WordCount.java ]
설명 : movies.csv 의 장르를 카운트 합니다.
실행 : hadoop jar WordCount.jar WordCount -Dmongo.input.uri=mongodb://[마스터노드의 내부아이피]:27017/[db이름].[collection이름] -Dmongo.output.uri=mongodb://[마스터노드의 내부아이피]:27017/[db이름].[collection이름]
예제 : hadoop jar WordCount.jar WordCount -Dmongo.input.uri=mongodb://10.140.0.8:27017/dataset.movielens -Dmongo.output.uri=mongodb://10.140.0.8:27017/dataset.out

나뉘어져 있는 이유 : short는 deprecated 된 API 를 사용하고 있는 코드입니다. 최신 API 를 사용하기 위해서, mongoDB 공식 예제에 있는 mapreduce 자바 코드를 가져와서 커스터마이징 한 것이 long에 있는 WordCount.java 입니다.

