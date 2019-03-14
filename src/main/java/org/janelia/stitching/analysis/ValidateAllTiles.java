package org.janelia.stitching.analysis;

import java.io.IOException;
import java.util.Arrays;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileLoader;
import org.janelia.stitching.TileLoader.TileType;

import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;

public class ValidateAllTiles
{
	public static void main( final String[] args ) throws IOException
	{
		final String tileConfigurationPath = args[ 0 ];
		System.out.println( "Input tile configuration: " + tileConfigurationPath + System.lineSeparator() );
		final DataProvider dataProvider = DataProviderFactory.create( DataProviderFactory.detectType( tileConfigurationPath ) );
		final TileInfo[] tileConfiguration = dataProvider.loadTiles( tileConfigurationPath );
		for ( final TileInfo tile : tileConfiguration )
		{
			final RandomAccessibleInterval< ? > tileImg;
			try
			{
				tileImg = TileLoader.loadTile( tile, dataProvider );
			}
			catch ( final Exception e )
			{
				throw new RuntimeException( "Failed to load tile #" + tile.getIndex() + ": " + tile.getFilePath(), e );
			}

			long numPixels = 0;
			if ( TileLoader.getTileType( tile, dataProvider ) == TileType.N5_DATASET )
			{
				// The tile is stored as N5 and was loaded as a lazy cell img, iterate over its cell grid
				final DatasetAttributes tileDatasetAttributes = TileLoader.getTileN5DatasetAttributes( tile, dataProvider );
				final CellGrid cellGrid = new CellGrid( tileDatasetAttributes.getDimensions(), tileDatasetAttributes.getBlockSize() );
				final long numCells = Intervals.numElements( cellGrid.getGridDimensions() );
				final long[] cellPos = new long[ cellGrid.numDimensions() ], cellMin = new long[ cellGrid.numDimensions() ], cellMax = new long[ cellGrid.numDimensions() ];
				final int[] cellDims = new int[ cellGrid.numDimensions() ];
				for ( long cellIndex = 0; cellIndex < numCells; ++cellIndex )
				{
					cellGrid.getCellGridPositionFlat( cellIndex, cellPos );
					cellGrid.getCellDimensions( cellPos, cellMin, cellDims );
					Arrays.setAll( cellMax, d -> cellMin[ d ] + cellDims[ d ] - 1 );
					final Interval cellInterval = new FinalInterval( cellMin, cellMax );
					final Cursor< ? > cellCursor = Views.interval( tileImg, cellInterval ).cursor();
					while ( cellCursor.hasNext() )
					{
						try
						{
							cellCursor.next();
						}
						catch ( final Exception e )
						{
							throw new RuntimeException( "Problem with loading cell " + Arrays.toString( cellPos ) + " of tile " + tile.getFilePath(), e );
						}
						++numPixels;
					}
				}
			}
			else
			{
				// The tile is stored as an image file, iterate over the entire image
				final Cursor< ? > cursor = Views.iterable( tileImg ).cursor();
				while ( cursor.hasNext() )
				{
					cursor.next();
					++numPixels;
				}
			}
			if ( numPixels != Intervals.numElements( tile.getSize() ) )
				throw new RuntimeException();

			System.out.println( "Loaded tile #" + tile.getIndex() + ", img size=" + Arrays.toString( Intervals.dimensionsAsLongArray( tileImg ) ) );
		}
		System.out.println( System.lineSeparator() + "Done" );
	}
}
