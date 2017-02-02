package org.janelia.stitching;

// TODO: add new test for shifted downsampling

public class DownsampleHistogramsTest
{
	/*private MultithreadedExecutor multithreadedExecutor;


	@Before
	public void setUp()
	{
		multithreadedExecutor = new MultithreadedExecutor();
	}

	@Test
	public void testHistogrammingThenDownsampling() throws Exception
	{
		final long[] size = new long[] { 2, 3 };
		final int entriesCount = 5;

		final TreeMap< Short, Integer >[] histograms = new TreeMap[ (int) (size[0]*size[1]) ];
		for ( int i = 0; i < histograms.length; i++ )
			histograms[i] = new TreeMap<>();

		histograms[0].put((short) 4, 3);
		histograms[0].put((short) 7, 2);

		histograms[1].put((short) 1, 1);
		histograms[1].put((short) 3, 3);
		histograms[1].put((short) 5, 1);

		histograms[2].put((short) 2, 2);
		histograms[2].put((short) 4, 2);
		histograms[2].put((short) 6, 1);

		histograms[3].put((short) 2,  1);
		histograms[3].put((short) 3,  1);
		histograms[3].put((short) 8,  2);
		histograms[3].put((short) 10, 1);

		histograms[4].put((short) 1, 2 );
		histograms[4].put((short) 3, 3 );

		histograms[5].put((short) 6, 2 );
		histograms[5].put((short) 7, 1 );
		histograms[5].put((short) 8, 1 );
		histograms[5].put((short) 9, 1 );



		final TreeMap< Short, Integer >[] expected = new TreeMap[ 1 ];
		for ( int i = 0; i < expected.length; i++ )
			expected[i] = new TreeMap<>();

		expected[ 0 ].put((short) 2, 1 );
		expected[ 0 ].put((short) 3, 1 );
		expected[ 0 ].put((short) 5, 1 );
		expected[ 0 ].put((short) 6, 1 );
		expected[ 0 ].put((short) 7, 1 );


		final TreeMap< Short, Integer >[] actual = IlluminationCorrectionHierarchical.downscaleHistograms(histograms, size, 0, entriesCount, multithreadedExecutor);

		try
		{
			Assert.assertEquals( expected.length, actual.length );
			Assert.assertArrayEquals( expected[0].keySet().toArray(), actual[0].keySet().toArray() );
			Assert.assertArrayEquals( expected[0].values().toArray(), actual[0].values().toArray() );
		}
		catch ( final AssertionError e )
		{
			System.out.println( String.format( "Expected length=%d, actual=%d;   histogram=%s, actual=%s", expected.length, actual.length, expected[ 0 ], actual[ 0 ] ) );
			throw e;
		}
	}


	@After
	public void tearDown()
	{
		multithreadedExecutor.shutdown();
	}*/
}
