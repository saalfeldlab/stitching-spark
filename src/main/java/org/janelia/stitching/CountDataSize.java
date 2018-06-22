package org.janelia.stitching;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.util.concurrent.MultithreadedExecutor;

public class CountDataSize
{
	public static void main( final String[] args ) throws Exception
	{
		try ( final MultithreadedExecutor threadPool = new MultithreadedExecutor() )
		{
			final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();
			for ( final String arg : args )
			{
				final TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( arg ) ) );
				final AtomicInteger err = new AtomicInteger();
				final long bytes = threadPool.sum(
						i -> {
							try
							{
								return Files.size( Paths.get( tiles[ i ].getFilePath() ) );
							}
							catch ( final IOException e )
							{
								err.incrementAndGet();
								return 0;
							}
						},
						tiles.length
					);
				final String[] units = new String[] { "b", "kb", "mb", "gb", "tb" };
				for ( int i = 0; i < units.length; ++i )
				{
					final long size = bytes << ( i * 10 );
					if ( size < 1000 || i == units.length - 1 )
					{
						System.out.println( String.format(
								"Config %s: %.2f %s%s",
								Paths.get( arg ).getFileName(),
								bytes / Math.pow( 2., i * 10 ),
								units[ i ],
								err.get() != 0 ? ", errors=" + err.get() : ""
							) );
						break;
					}
				}
			}
		}
	}
}
