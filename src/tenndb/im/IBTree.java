package tenndb.im;

import java.util.List;

import tenndb.dm.UUID;
import tenndb.index.BTNode;


public interface IBTree {
		
	public boolean insert(UUID key, UUID var);
	
	public UUID update(UUID key, UUID var);
	
//	public IndexBlock delete(int key);
	
	public UUID search(UUID key);

	public int count(UUID fromKey, UUID toKey);
	
	public List<UUID> range(UUID fromKey, UUID toKey, boolean isInc);
	
	//
    public void print(List<String> strList);
    
    public String toString();
    
	public void printNext();
	
	public void printTreePrior();
	
	//
	public UUID getRoot();
	
	public BTNode seekNode(UUID key);
	
}