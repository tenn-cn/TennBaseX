package tenndb.index.test;

import java.util.ArrayList;
import java.util.List;

import tenndb.dm.UUID;
import tenndb.im.IBTree;
import tenndb.index.BTNode;
import tenndb.index.RIBTree;

public class TestIBTree {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		RIBTree tree = new RIBTree();
		

		

		for(int t = 0; t < 50; ++t){
			
			long t1 = Runtime.getRuntime().totalMemory();
			{
				long a = Runtime.getRuntime().maxMemory();
				
				System.out.println(t + ", a: m = " + a + ", t = " + t1 );
			}
			
			for(int i = 1; i < 100000; ++i)
			{
				UUID key = new UUID(i + t * 100000);
				tree.insert(key, key);
			}
			
			long t2 = Runtime.getRuntime().totalMemory();
			{
				long a = Runtime.getRuntime().maxMemory();
			
				System.out.println(t + ", b: m = " + a + ", t = " + t2 );
			}
			
			int leaves = 0;
			for(BTNode node : tree.getList()){
				if(node.isLeaf()){
					leaves ++;
				}
			}
			
			System.out.println(t + ", c: cost = " + (t2 - t1) + ", size = " + tree.getList().size() + ", leaves = " + leaves);
			
		}
		
		
		
/*		tree.printNext();
		
		List<String> strList = new ArrayList<String>();
		tree.print(strList);
		if(null != strList){
			for(String str : strList){
				System.out.println(str);
			}
		}*/
		
/*		for(int i = 1; i < 10000; i*=2){
			UUID key = new UUID(i);
			UUID value1 = new UUID(i + 10000);
			tree.update(key, value1);
			UUID value2 = tree.search(key);
			System.out.println("key = " + key.getUID() + ", value = " + value2.getUID());
		}*/
		
		System.out.println();

	}

}
