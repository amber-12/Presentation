import java.util.Comparator;


public class EmpComp1 implements Comparator<Employ>{

	@Override
	public int compare(Employ o1, Employ o2) {
		// TODO Auto-generated method stub
		if(o1.id>o2.id)
		{
			return 1;
		}
		else if(o1.id<o2.id)
		{
			return -1;
		}
		else
		{
		return 0;
		}
	}
	

}
