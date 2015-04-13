package ch.epfl.data.plan_runner.thetajoin.matrix_mapping;

import java.util.List;

import ch.epfl.data.plan_runner.thetajoin.matrix_mapping.MatrixAssignment.Dimension;

/**
 * Interface for hypercube partitioning and assignment
 * 
 * @author Tam
 * @param <KeyType>
 */
public interface HyperCubeAssignment<KeyType> {

	public class Dimension {
		private int val;
		
		private Dimension(int oneToN){
			this.val = oneToN;
		}
		
		public int val() {
			return val;
		}
		
		public static Dimension d(int oneToN){
			return new Dimension(oneToN);
		}
	};

	/**
	 * This method is used to get a list of reducers to which a given tuple must
	 * be sent.
	 * 
	 * @param dim
	 *            indicate from which relation the tuple comes.
	 * @return a list of index of reducers.
	 */
	public List<Integer> getRegionIDs(Dimension dim);
	
	/**
	 * This method is used to get a list of reducers to which a given tuple must
	 * be sent to given a key.
	 * 
	 * @param dim
	 *            indicate from which relation the tuple comes.
	 * @return a list of index of reducers.
	 */
	public List<Integer> getRegionIDs(Dimension dim, KeyType key);
	
	/**
	 * Return the number of region divisions in a given dimension perspective
	 */
	public int getNumberOfRegions(Dimension dim);
	
	/**
	 * Return the partitioning result across each dimension
	 */
	public String getMappingDimensions();
	
}
