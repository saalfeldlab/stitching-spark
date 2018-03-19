package org.janelia.stitching;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;

public class C1WarpedStitchingJob implements Serializable {

	private static final long serialVersionUID = 2619120742300093982L;

	private final C1WarpedStitchingArguments args;

	private final TileSlabMapping tileSlabMapping;

	private final Set< Integer > filteredTileIndexes;

	private transient DataProvider dataProvider;

	private SerializableStitchingParameters params;

	private String saveFolder;
	private String datasetName;

	public C1WarpedStitchingJob( final TileSlabMapping tileSlabMapping )
	{
		this( tileSlabMapping, null );
	}

	public C1WarpedStitchingJob( final TileSlabMapping tileSlabMapping, final Set< Integer > filteredTileIndexes )
	{
		this.tileSlabMapping = tileSlabMapping;
		this.filteredTileIndexes = filteredTileIndexes;

		dataProvider = DataProviderFactory.createFSDataProvider();
		args = new C1WarpedStitchingArguments();
	}

	public synchronized DataProvider getDataProvider()
	{
		if ( dataProvider == null )
			dataProvider = DataProviderFactory.createFSDataProvider();
		return dataProvider;
	}

	public C1WarpedStitchingArguments getArgs() { return args; }
	public TileSlabMapping getTileSlabMapping() { return tileSlabMapping; }

	public SerializableStitchingParameters getParams() { return params; }
	public void setParams( final SerializableStitchingParameters params ) { this.params = params; }

	public int getChannels() { return C1WarpedMetadata.NUM_CHANNELS; }

	public TileInfo[] getTiles( final int channel ) throws IOException
	{
		final TileInfo[] allTiles = C1WarpedMetadata.getTiles( channel );
		if ( filteredTileIndexes == null )
			return allTiles;

		final List< TileInfo > filteredTiles = new ArrayList<>();
		for ( final TileInfo tile : allTiles )
			if ( filteredTileIndexes.contains( tile.getIndex() ) )
				filteredTiles.add( tile );
		return filteredTiles.toArray( new TileInfo[ 0 ] );
	}

	public String getFlatfieldPath( final int channel ) { return C1WarpedMetadata.getFlatfieldPath( channel ); };

	public String getSaveFolder() { return saveFolder; }
	public void setSaveFolder( final String saveFolder ) { this.saveFolder = saveFolder; }

	public String getBasePath() { return C1WarpedMetadata.getBasePath(); }
	public String getDatasetName() { return datasetName; }

	public int getDimensionality() { return C1WarpedMetadata.NUM_DIMENSIONS; }
	public double[] getPixelResolution() throws IOException { return getTiles( 0 )[ 0 ].getPixelResolution(); }
}
