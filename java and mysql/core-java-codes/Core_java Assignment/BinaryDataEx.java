import java.io.FileOutputStream;


public class BinaryDataEx {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String datafile="F:\\BinaryData.txt";
		
		String bytedata="this is example of binary data file";
		
		byte[] buffer=bytedata.getBytes();
		
		FileOutputStream fous=null;
				
		try
		{
			fous=new FileOutputStream(datafile);
			fous.write(buffer);
			fous.close();
		}
       catch(Exception e)
		{
    	   System.out.println(e.getMessage());
		}
		finally
		{
			System.out.println("sucessful");
		}
	}

}
