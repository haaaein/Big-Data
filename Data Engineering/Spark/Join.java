import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.regex.*;

public final class JavaJoin {
	public static class Product implements Serializable {
		public int id;
		public int price;
		public String code;
		
		public Product(int id, int price, String code) {
			this.id = id;
			this.price = price;
			this.code = code;
		}
		
		public String toString() {
			return "id : " + id + ", price : " + price + ", code : " + code;
		}
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			System.err.println("Usage: JavaJoin");
			System.exit(1);
		}
		
		SparkSession spark = SparkSession
				.builder()
				.appName("JavaJoin")
				.getOrCreate();
		
		JavaRDD<String> products = spark.read().textFile(args[0]).javaRDD();
		PairFunction<String, String, Product> pfA = new PairFunction<String, String, Product>() {
			public Tuple2<String, Product> call(String s) {
				Sting[] pValues = s.split("\\|");
				return new Tuple2(pValues[2], new Product(Integer.parseInt(pValues[0]), Integer.parseInt(pValues[1]), pValues[2]));
			}
		};
		JavaRDD<String, Product> pTuples = products.mapToPair(pfA);
		
		JavaRDD<String> codes = spark.read().textFile(args[1]).javaRDD();
		PairFunction<String, String, Code> pfB = new PairFunction<String, String, Code>() {
			public Tuple2<String, Code> call(String s) {
				String[] cValues = s.split("\\|");
				return new Tuple2(cValues[0], new Code(cValues[0], cValues[1]));
			}
		};
		JavaPairRDD<String, Code> cTuples = codes.mapToPair(pfB);
		
		JavaPairRDD<String, Tuple2<Product, Code>> joined = pTuples.join(cTuples);
		JavaPairRDD<String, Tuple2<Product, Optional<Code>>> leftOuterJoined = pTuples.leftOuterJoin(cTuples);
		JavaPairRDD<String, Tuple2<Optional<Product>, Code>> rightOuterJoined = pTuples.rightOuterJoin(cTuples);
		JavaPairRdd<String, Tuple2<Optional<Product>, Optional<Code>>> fullOuterJoined = pTuples.fullOuterJoin(cTuples);
		
		joined.saveAsTextFile(args[args.length - 1] + "_join");
		leftOuterJoined.saveAsTextFile(args[args.length - 1] + "_leftOuterJoin");
		rightOuterJoined.saveAsTextFile(args[args.length - 1] + "_rightOuterJoin");
		fullOuterJoined.saveAsTextFile(args[args.length - 1] + "_fullOuterJoin");
		
		spark.stop();
	}
	
	
}
