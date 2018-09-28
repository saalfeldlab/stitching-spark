package mpicbg.models;

public class IndexedTile< M extends Model< M > > extends Tile< M >
{
	private static final long serialVersionUID = -2283062844945777230L;

	private final int index;

	public IndexedTile( final M model, final int index )
	{
		super( model );
		this.index = index;
	}

	public int getIndex()
	{
		return index;
	}
}
